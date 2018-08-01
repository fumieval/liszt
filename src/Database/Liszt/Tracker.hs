{-# LANGUAGE DeriveGeneric #-}
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
import Database.Liszt.Internal
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Internal as B
import qualified Data.IntMap.Strict as IM
import qualified Data.HashMap.Strict as HM
import Data.Scientific (Scientific)
import Data.Reflection (Given(..), give)
import Data.Text (Text)
import Data.Winery
import Foreign.ForeignPtr
import qualified Data.Winery.Internal.Builder as WB
import GHC.Generics (Generic)
import System.Directory
import System.FilePath
import System.FSNotify
import System.IO

data Offset = SeqNo !Int
  | FromEnd !Int
  | WineryTag !Schema ![Text] !Scientific
  deriving (Show, Generic)
instance Serialise Offset

data Request = Request
  { reqKey :: !Key
  , reqTimeout :: !Int
  , reqLimit :: !Int
  , reqFrom :: !Offset
  , reqTo :: !Offset
  } deriving (Show, Generic)
instance Serialise Request

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
  | WinerySchemaError !String
  | WineryError !DecodeException
  deriving (Show, Read)
instance Exception LisztError

data Tracker = Tracker
  { vRoot :: !(TVar (Frame CachePointer))
  , vUpdated :: !(TVar Bool)
  , vPending :: !(TVar [STM (IO ())])
  , vReaders :: !(TVar Int)
  , followThread :: !ThreadId
  , trackerPath :: !B.ByteString
  , streamHandle :: !LisztHandle
  , cache :: !Cache
  }

data Cache = Cache
  { primaryCache :: TVar (IM.IntMap (Frame CachePointer))
  , secondaryCache :: TVar (IM.IntMap (Frame CachePointer))
  }

newtype CachePointer = CachePointer RawPointer

instance Given Cache => Fetchable CachePointer where
  fetchFrame h (CachePointer p@(RP ofs _)) = join $ atomically $ do
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
            x <- fmap CachePointer <$> fetchFrame h p
            atomically $ modifyTVar' primaryCache $ IM.insert ofs x
            return x

flipCache :: Cache -> STM ()
flipCache Cache{..} = do
  readTVar primaryCache >>= writeTVar secondaryCache
  writeTVar primaryCache IM.empty

createTracker :: WatchManager -> FilePath -> B.ByteString -> IO Tracker
createTracker man prefix trackerPath = do
  let filePath = prefix </> B.unpack trackerPath
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

  fptr <- B.mallocByteString 4096

  let wait = atomically $ do
        b <- readTVar vUpdated
        unless b retry
        writeTVar vUpdated False

  hSeek (hPayload streamHandle) SeekFromEnd (-fromIntegral footerSize)
  let seekRoot prevSize = do
        n <- withForeignPtr fptr $ \p -> hGetBuf (hPayload streamHandle) p 4096
        if n == 0
          then do
            let bs = B.PS fptr (max 0 $ prevSize - footerSize) footerSize
            if isFooter bs
              then try (evaluate $ decodeFrame bs) >>= \case
                Left (_ :: DecodeException) -> do
                  wait
                  seekRoot 0
                Right a -> forceSpine a
              else do
                wait
                seekRoot 0
          else seekRoot n

  cache <- Cache <$> newTVarIO IM.empty <*> newTVarIO IM.empty

  followThread <- forkFinally (forever $ do
    newRoot <- fmap CachePointer <$> seekRoot 0
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
  , vTrackers :: TVar (HM.HashMap B.ByteString Tracker)
  , prefix :: FilePath
  }

withLisztReader :: FilePath -> (LisztReader -> IO ()) -> IO ()
withLisztReader prefix k = do
  vTrackers <- newTVarIO HM.empty
  withManager $ \watchManager -> k LisztReader{..}

acquireTracker :: LisztReader -> B.ByteString -> IO Tracker
acquireTracker LisztReader{..} path = join $ atomically $ do
  streams <- readTVar vTrackers
  case HM.lookup path streams of
    Just s -> do
      modifyTVar' (vReaders s) (+1)
      return (return s)
    Nothing -> return $ do
      s <- createTracker watchManager prefix path
      atomically $ modifyTVar vTrackers (HM.insert path s)
      return s

releaseTracker :: LisztReader -> Tracker -> IO ()
releaseTracker LisztReader{..} Tracker{..} = join $ atomically $ do
  n <- readTVar vReaders
  if n <= 1
    then do
      modifyTVar' vTrackers (HM.delete trackerPath)
      return $ do
        killThread followThread
        closeLiszt streamHandle
    else return () <$ writeTVar vReaders (n - 1)

withTracker :: LisztReader -> B.ByteString -> (Tracker -> IO a) -> IO a
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
                WineryTag sch name p -> do
                  dec <- handleWinery sch name
                  takeSpineWhile ((>=p) . dec . WB.toByteString) streamHandle spine' [] >>= cont streamHandle ofs
      case reqTo of
        FromEnd ofs -> goSeqNo (len - ofs)
        SeqNo ofs -> goSeqNo ofs
        WineryTag sch name p -> do
          dec <- handleWinery sch name
          dropSpineWhile ((>=p) . dec . WB.toByteString) streamHandle spine >>= \case
            Nothing -> cont streamHandle 0 []
            Just (dropped, e, spine') -> case reqFrom of
              FromEnd n -> takeSpine streamHandle (min reqLimit $ n - dropped + 1) spine' [e] >>= cont streamHandle (len - dropped)
              SeqNo n -> takeSpine streamHandle (min reqLimit $ len - dropped - n + 1) spine' [e] >>= cont streamHandle (len - dropped)
              WineryTag sch' name' q -> do
                dec' <- handleWinery sch' name'
                takeSpineWhile ((>=q) . dec' . WB.toByteString) streamHandle spine' [e] >>= cont streamHandle (len - dropped)
  where
    handleWinery :: Schema -> [Text] -> IO (B.ByteString -> Scientific)
    handleWinery sch names = either (throwIO . WinerySchemaError . show) pure
      $ getDecoderBy (foldr (flip extractFieldBy) deserialiser names) sch
