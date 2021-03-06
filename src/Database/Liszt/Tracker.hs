{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Liszt.Tracker
  ( Offset(..)
  , Request(..)
  , defRequest
  , handleRequest
  , LisztError(..)
  , LisztReader
  , withLisztReader
  , Tracker
  , withTracker
  )where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Control.Exception
import Control.Monad
import Deriving.Aeson
import Deriving.Aeson.Stock
import Database.Liszt.Internal
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Internal as B
import qualified Data.IntMap.Strict as IM
import qualified Data.HashMap.Strict as HM
import Data.Scientific (Scientific)
import Data.Reflection (Given(..), give)
import Data.Text (Text)
import qualified Data.Text.Encoding as T
import Foreign.ForeignPtr
import GHC.Generics (Generic)
import System.Directory
import System.FilePath
import System.FSNotify
import System.IO

data Offset = SeqNo !Int
  | FromEnd !Int
  deriving (Show, Generic)
  deriving (FromJSON, ToJSON) via CustomJSON '[SumObjectWithSingleField] Offset

data Request = Request
  { reqKey :: !Key
  , reqTimeout :: !Int
  , reqLimit :: !Int
  , reqFrom :: !Offset
  , reqTo :: !Offset
  } deriving (Show, Generic)
  deriving (FromJSON, ToJSON) via PrefixedSnake "req" Request

defRequest :: Key -> Request
defRequest k = Request
  { reqKey = k
  , reqTimeout = 0
  , reqFrom = FromEnd 1
  , reqTo = FromEnd 1
  , reqLimit = maxBound
  }

data LisztError = MalformedRequest
  | InvalidRequest
  | StreamNotFound
  | FileNotFound
  | IndexNotFound
  deriving Show
instance Exception LisztError

data Tracker = Tracker
  { vRoot :: !(TVar (Node RawPointer))
  , vUpdated :: !(TVar Bool)
  , vPending :: !(TVar [STM (IO ())])
  , vReaders :: !(TVar Int)
  , followThread :: !ThreadId
  , filePath :: !FilePath
  , streamHandle :: !LisztHandle
  , cache :: !Cache
  }

data Cache = Cache
  { primaryCache :: TVar (IM.IntMap (Node RawPointer))
  , secondaryCache :: TVar (IM.IntMap (Node RawPointer))
  }

newtype CachedHandle = CachedHandle LisztHandle

instance Given Cache => Fetchable CachedHandle where
  fetchNode (CachedHandle h) p@(RP ofs _) = join $ atomically $ do
    let Cache{..} = given
    pcache <- readTVar primaryCache
    case IM.lookup ofs pcache of
      Just x -> return (pure x)
      Nothing -> do
        scache <- readTVar secondaryCache
        case IM.lookup ofs scache of
          Just x -> do
            writeTVar primaryCache $! IM.insert ofs x pcache
            return (pure x)
          Nothing -> return $ do
            x <- fetchNode h p
            atomically $ modifyTVar' primaryCache $ IM.insert ofs x
            return x
  fetchKey (CachedHandle h) k = fetchKey h k
  fetchRoot (CachedHandle h) = fetchRoot h
  fetchPayload = error "fetchPayload"

flipCache :: Cache -> STM ()
flipCache Cache{..} = do
  readTVar primaryCache >>= writeTVar secondaryCache
  writeTVar primaryCache IM.empty

createTracker :: WatchManager -> FilePath -> IO Tracker
createTracker man filePath = do
  exist <- doesFileExist filePath
  unless exist $ throwIO FileNotFound
  streamHandle <- openLiszt filePath
  vRoot <- newTVarIO Empty
  vPending <- newTVarIO []
  vUpdated <- newTVarIO True
  vReaders <- newTVarIO 1
  stopWatch <- watchDir man (takeDirectory filePath) (\case
    Modified path' _ _ | filePath == path' -> True
    _ -> False)
    $ const $ void $ atomically $ writeTVar vUpdated True

  let wait = atomically $ do
        b <- readTVar vUpdated
        unless b retry
        writeTVar vUpdated False

  let seekRoot = do
        hSeek (hPayload streamHandle) SeekFromEnd (-fromIntegral footerSize)
        bs@(B.PS fp _ _) <- B.hGet (hPayload streamHandle) footerSize
        if isFooter bs
          then try (withForeignPtr fp peekNode) >>= \case
            Left LisztDecodingException -> do
              putStrLn "WARNING: isFooter passed, but it's not valid"
              wait
              seekRoot
            Right a -> return a
          else wait >> seekRoot

  cache <- Cache <$> newTVarIO IM.empty <*> newTVarIO IM.empty

  followThread <- forkFinally (forever $ do
    newRoot <- seekRoot
    join $ atomically $ do
      flipCache cache
      writeTVar vRoot newRoot
      pending <- readTVar vPending
      writeTVar vPending []
      ms <- sequence pending
      return $ sequence_ ms
    wait) $ const $ stopWatch >> closeLiszt streamHandle

  return Tracker{..}

data LisztReader = LisztReader
  { watchManager :: WatchManager
  , vTrackers :: TVar (HM.HashMap FilePath Tracker)
  }

withLisztReader :: (LisztReader -> IO ()) -> IO ()
withLisztReader k = do
  vTrackers <- newTVarIO HM.empty
  withManager $ \watchManager -> k LisztReader{..}

acquireTracker :: LisztReader -> FilePath -> IO Tracker
acquireTracker LisztReader{..} path = join $ atomically $ do
  streams <- readTVar vTrackers
  case HM.lookup path streams of
    Just s -> do
      modifyTVar' (vReaders s) (+1)
      return (return s)
    Nothing -> return $ do
      s <- createTracker watchManager path
      atomically $ modifyTVar vTrackers (HM.insert path s)
      return s

releaseTracker :: LisztReader -> Tracker -> IO ()
releaseTracker LisztReader{..} Tracker{..} = join $ atomically $ do
  n <- readTVar vReaders
  if n <= 1
    then do
      modifyTVar' vTrackers (HM.delete filePath)
      return $ do
        killThread followThread
        closeLiszt streamHandle
    else return () <$ writeTVar vReaders (n - 1)

withTracker :: LisztReader -> FilePath -> (Tracker -> IO a) -> IO a
withTracker env path = bracket (acquireTracker env path) (releaseTracker env)

handleRequest :: Tracker
  -> Request
  -> (LisztHandle -> Int -> [QueryResult] -> IO ())
  -> IO ()
handleRequest str@Tracker{..} req@Request{..} cont = do
  root <- atomically $ do
    b <- readTVar vUpdated
    when b retry
    readTVar vRoot
  give cache $ lookupSpine streamHandle reqKey root >>= \case
    Nothing -> throwIO StreamNotFound
    Just spine -> do
      let len = spineLength spine
      let goSeqNo ofs
            | ofs >= len = do
              delay <- newDelay reqTimeout
              atomically $ do
                modifyTVar vPending $ (:) $ cont streamHandle 0 [] <$ waitDelay delay
                  <|> pure (handleRequest str req { reqTo = SeqNo ofs } cont)
            | otherwise = do
              spine' <- dropSpine streamHandle (len - ofs - 1) spine
              case reqFrom of
                FromEnd n -> takeSpine streamHandle (min reqLimit $ ofs - (len - n) + 1) spine' [] >>= cont streamHandle ofs
                SeqNo n -> takeSpine streamHandle (min reqLimit $ ofs - n + 1) spine' [] >>= cont streamHandle ofs
      case reqTo of
        FromEnd ofs -> goSeqNo (len - ofs)
        SeqNo ofs -> goSeqNo ofs
