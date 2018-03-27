{-# LANGUAGE RecordWildCards, LambdaCase, OverloadedStrings, ViewPatterns, BangPatterns #-}
module Database.Liszt.Server (System
    , pushPayload
    , MonotonicityViolation
    , openLisztServer) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Binary as B
import Data.Binary.Get as B
import Data.Binary.Put as B
import Data.Int
import Data.Semigroup
import Data.String
import Database.Liszt.Types
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Sequence as S
import qualified Network.WebSockets as WS
import System.Directory
import System.IO

data System = System
    { vPayload :: TVar (S.Seq (B.ByteString, Int64))
    -- ^ A collection of payloads which are not available on disk.
    , vOffsets :: TVar (S.Seq Int64)
    -- ^ Map of byte offsets
    , vAccess :: TVar Bool
    -- ^ Is the payload file in use?
    , theHandle :: Handle
    -- ^ The handle of the payload
    }

-- | Set the 'TVar' True while running the action.
-- It blocks if the TVar is already True.
acquire :: TVar Bool -> IO a -> IO a
acquire v m = do
  atomically $ do
    b <- readTVar v
    if b then retry else writeTVar v True
  m `finally` atomically (writeTVar v False)

-- | Read the payload at the position in 'TVar', and update it by the next position.
fetchPayload :: System
  -> TVar (Int, Int64)
  -> STM (IO (Int, B.ByteString))
fetchPayload System{..} v = do
  (ofs, pos) <- readTVar v
  m <- readTVar vOffsets
  case S.lookup ofs m of
    Just pos' -> do
      writeTVar v (ofs + 1, pos')
      return $ acquire vAccess $ do
        hSeek theHandle AbsoluteSeek (fromIntegral pos)
        bs <- B.hGet theHandle $ fromIntegral $ pos' - pos
        return (ofs, bs)
    Nothing -> S.lookup (ofs - S.length m) <$> readTVar vPayload >>= \case
      Just (bs, pos') -> do
        writeTVar v (ofs + 1, pos')
        return $ return (ofs, bs)
      Nothing -> retry

handleConsumer :: System -> WS.Connection -> IO ()
handleConsumer sys@System{..} conn = do
  -- start from the beginning of the stream
  vOffset <- newTVarIO (0, 0)

  let transaction (NonBlocking r) = transaction r
        <|> pure (WS.sendTextData conn ("EOF" :: BL.ByteString))
      transaction Read = (>>= \(i, bs) -> WS.sendBinaryData conn $ B.runPut $ B.put i >> B.putByteString bs )
          <$> fetchPayload sys vOffset
      transaction Peek = WS.sendBinaryData conn . encode . fst
          <$> readTVar vOffset
      transaction (Seek ofs) = do
        m <- readTVar vOffsets
        let i = fromIntegral ofs
        writeTVar vOffset $ maybe (0, 0) ((,) i) $ S.lookup i m
        return $ return ()

  forever $ do
    req <- WS.receiveData conn
    case decodeOrFail req of
      Right (_, _, r) -> join $ atomically $ transaction r
      Left (_, _, e) -> WS.sendClose conn (fromString e :: B.ByteString)

-- | The final offset.
getLastOffset :: System -> STM Int64
getLastOffset System{..} = readTVar vPayload >>= \m -> case S.viewr m of
  _ S.:> (_, p) -> return p
  S.EmptyR -> readTVar vOffsets >>= \n -> case S.viewr n of
    _ S.:> p -> return p
    S.EmptyR -> return 0

data MonotonicityViolation = MonotonicityViolation deriving Show
instance Exception MonotonicityViolation

-- | Push a payload.
pushPayload :: System -> B.ByteString -> STM ()
pushPayload sys@System{..} content = do
  p <- getLastOffset sys

  let !p' = p + fromIntegral (B.length content)
  modifyTVar' vPayload (S.|> (content, p'))

handleProducer :: System -> WS.Connection -> IO ()
handleProducer sys@System{..} conn = forever $ do
  reqBS <- WS.receiveData conn
  case runGetOrFail get reqBS of
    Right (BL.toStrict -> !content, _, req) -> join $ atomically $ case req of
      WriteSeqNo -> return () <$ pushPayload sys content

    Left _ -> WS.sendClose conn ("Malformed request" :: B.ByteString)

loadIndices :: FilePath -> IO (S.Seq Int64)
loadIndices path = doesFileExist path >>= \case
  False -> return S.empty
  True -> do
    n <- (`div`8) <$> fromIntegral <$> getFileSize path
    bs <- BL.readFile path
    return $! S.fromList $ runGet (replicateM n get) bs

synchronise :: FilePath -> System -> IO ()
synchronise ipath System{..} = forever $ do
  m <- atomically $ do
    w <- readTVar vPayload
    when (S.null w) retry
    return w

  acquire vAccess $ do
    hSeek theHandle SeekFromEnd 0
    mapM_ (B.hPut theHandle . fst) m
    hFlush theHandle

  BL.appendFile ipath
    $ B.runPut $ forM_ m (B.put . snd)

  atomically $ do
    modifyTVar' vOffsets (S.>< fmap snd m)
    modifyTVar' vPayload (S.drop (S.length m))

-- | Start a liszt server. 'openLisztServer "foo"' creates two files:
--
-- * @foo.indices@: A list of offsets.
--
-- * @foo.payload@: All payloads concatenated as one file.
--
openLisztServer :: FilePath -> IO (System, IO (), WS.ServerApp)
openLisztServer path = do
  let ipath = path ++ ".indices"
  let ppath = path ++ ".payload"

  vOffsets <- loadIndices ipath >>= newTVarIO

  vPayload <- newTVarIO S.empty

  vAccess <- newTVarIO False

  theHandle <- openBinaryFile ppath ReadWriteMode
  hSetBuffering theHandle (BlockBuffering Nothing)

  let sys = System{..}

  return (sys
    , forever $ synchronise ipath sys
      `catch` \e -> hPutStrLn stderr $ "synchronise: " ++ show (e :: IOException)
    , \pending -> case WS.requestPath (WS.pendingRequest pending) of
    "read" -> WS.acceptRequest pending >>= handleConsumer sys
    "write" -> WS.acceptRequest pending >>= handleProducer sys
    p -> WS.rejectRequest pending ("Bad request: " <> p))
