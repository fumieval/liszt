{-# LANGUAGE DeriveGeneric, RecordWildCards, LambdaCase, Rank2Types, ScopedTypeVariables #-}
module Database.Liszt (
    openLiszt,
    closeLiszt,
    withLiszt,
    LisztHandle,
    -- * Writer interface
    Key,
    Tag,
    Transaction,
    clear,
    insert,
    commit,
    commitFile,
    -- * Reader
    Request(..),
    Offset(..),
    defRequest,
    IndexMap,
    LisztError(..),
    LisztReader,
    Tracker,
    acquireTracker,
    releaseTracker,
    withLisztReader,
    handleRequest,
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Control.Exception
import Control.Monad
import Database.Liszt.Internal
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Internal as B
import qualified Data.HashMap.Strict as HM
import Data.Scientific (Scientific)
import Data.Text (Text)
import Data.Winery
import Foreign.ForeignPtr
import qualified Data.Winery.Internal.Builder as WB
import GHC.Generics (Generic)
import System.Directory
import System.FilePath
import System.FSNotify
import System.IO

commitFile :: FilePath -> Transaction a -> IO a
commitFile path m = withLiszt path $ \h -> commit h m

data Offset = Count !Int
  | SeqNo !Int
  | FromEnd !Int
  | WineryTag !Schema !Text !Scientific
  deriving (Show, Generic)
instance Serialise Offset

data Request = Request
  { reqKey :: !Key
  , reqTimeout :: !Int
  , reqFrom :: !Offset
  , reqTo :: !Offset
  } deriving (Show, Generic)
instance Serialise Request

defRequest :: Key -> Request
defRequest k = Request
  { reqKey = k
  , reqTimeout = 0
  , reqFrom = Count 1
  , reqTo = FromEnd 1
  }

type IndexMap = HM.HashMap B.ByteString

data Tracker = Tracker
  { vRoot :: TVar (Frame RawPointer)
  , vUpdated :: TVar Bool
  , vPending :: TVar [STM (IO ())]
  , vReaders :: TVar Int
  , followThread :: ThreadId
  , streamHandle :: LisztHandle
  }

releaseTracker :: Tracker -> IO ()
releaseTracker Tracker{..} = do
  killThread followThread
  closeLiszt streamHandle

createTracker :: WatchManager -> FilePath -> IO Tracker
createTracker man path = do
  exist <- doesFileExist path
  unless exist $ throwIO FileNotFound
  streamHandle <- openLiszt path
  vRoot <- newTVarIO Empty
  vPending <- newTVarIO []
  vUpdated <- newTVarIO True
  vReaders <- newTVarIO 1
  stopWatch <- watchDir man (takeDirectory path) (\case
    Modified path' _ _ | path == path' -> True
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
                Left (e :: DecodeException) -> do
                  wait
                  seekRoot 0
                Right a -> forceSpine a
              else do
                wait
                seekRoot 0
          else seekRoot n

  followThread <- forkFinally (forever $ do
    newRoot <- seekRoot 0
    join $ atomically $ do
      writeTVar vRoot newRoot
      pending <- readTVar vPending
      writeTVar vPending []
      ms <- sequence pending
      return $ sequence_ ms
    wait) $ const $ stopWatch >> closeLiszt streamHandle

  return Tracker{..}

data LisztError = MalformedRequest
  | InvalidRequest
  | TrackerNotFound
  | FileNotFound
  | IndexNotFound
  | WinerySchemaError !String
  | WineryError !DecodeException
  deriving (Show, Read)
instance Exception LisztError

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
      s <- createTracker watchManager (prefix </> B.unpack path)
      atomically $ modifyTVar vTrackers (HM.insert path s)
      return s

handleRequest :: Tracker
  -> Request
  -> (LisztHandle -> Int -> [QueryResult] -> IO ())
  -> IO ()
handleRequest str@Tracker{..} req@Request{..} cont = do
  root <- atomically $ do
    b <- readTVar vUpdated
    when b retry
    readTVar vRoot
  lookupSpine streamHandle reqKey root >>= \case
    Nothing -> throwIO TrackerNotFound
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
                Count n -> takeSpine streamHandle n spine' [] >>= cont streamHandle ofs
                FromEnd n -> takeSpine streamHandle (ofs - (len - n) + 1) spine' [] >>= cont streamHandle ofs
                SeqNo n -> takeSpine streamHandle (ofs - n + 1) spine' [] >>= cont streamHandle ofs
                WineryTag sch name p -> do
                  dec <- handleWinery sch name
                  takeSpineWhile ((>=p) . dec . WB.toByteString) streamHandle spine' [] >>= cont streamHandle ofs
      case reqTo of
        Count _ -> throwIO InvalidRequest
        FromEnd ofs -> goSeqNo (len - ofs)
        SeqNo ofs -> goSeqNo ofs
        WineryTag sch name p -> do
          dec <- handleWinery sch name
          dropSpineWhile ((>=p) . dec . WB.toByteString) streamHandle spine >>= \case
            Nothing -> cont streamHandle 0 []
            Just (dropped, e, spine') -> case reqFrom of
              Count n -> takeSpine streamHandle n spine' [e] >>= cont streamHandle (len - dropped)
              FromEnd n -> takeSpine streamHandle (n - dropped + 1) spine' [e] >>= cont streamHandle (len - dropped)
              SeqNo n -> takeSpine streamHandle (len - dropped - n + 1) spine' [e] >>= cont streamHandle (len - dropped)
              WineryTag sch' name' q -> do
                dec' <- handleWinery sch' name'
                takeSpineWhile ((>=q) . dec' . WB.toByteString) streamHandle spine' [e] >>= cont streamHandle (len - dropped)
  where
    handleWinery :: Schema -> Text -> IO (B.ByteString -> Scientific)
    handleWinery sch name = either (throwIO . WinerySchemaError . show) pure
      $ getDecoderBy (extractField name) sch
