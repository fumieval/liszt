{-# LANGUAGE RecordWildCards, LambdaCase, OverloadedStrings, ViewPatterns, BangPatterns #-}
module Database.Liszt.Server (Stream
    , Server
    , pushPayload
    , MonotonicityViolation
    , openLisztServer) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Binary as B
import Data.Binary.Get as B
import Data.Binary.Put as B
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Semigroup
import Data.String
import Database.Liszt.Types
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Sequence as S
import qualified Network.WebSockets as WS
import System.Directory
import System.FilePath
import System.IO

data Server = Server
    { vStreams :: TVar (HM.HashMap StreamId Stream)
    , vNewStream :: TVar Bool
    , serverPrefix :: FilePath
    }

data Stream = Stream
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
fetchPayload :: Stream -> Int64 -> STM (IO (Int, B.ByteString))
fetchPayload Stream{..} (fromIntegral -> ofs0) = do
  m <- readTVar vOffsets
  let len = S.length m
  let ofs
          | ofs0 < 0 = len + ofs0
          | otherwise = ofs0
  case ofs < len of
    True | pos' <- S.index m ofs
        , pos <- if ofs == 0
            then 0
            else S.index m (ofs - 1) -> do
      return $ acquire vAccess $ do
        hSeek theHandle AbsoluteSeek (fromIntegral pos)
        bs <- B.hGet theHandle $ fromIntegral $ pos' - pos
        return (ofs, bs)
    False -> readTVar vPayload >>= \p -> case ofs - len < S.length p of
      True | (bs, _) <- S.index p (ofs - len) -> do
        return $ return (ofs, bs)
      False -> retry

handleConsumer :: Server -> WS.Connection -> IO ()
handleConsumer Server{..} conn = do
  let transaction (Read s o b) = do
        ss <- readTVar vStreams
        case HM.lookup s ss of
          Just stream -> do
            m <- case o of
              Offset i -> fetchPayload stream i
            return $ do
              (i, bs) <- m
              WS.sendBinaryData conn $ B.runPut $ B.put i >> B.putByteString bs
          Nothing -> return $ WS.sendTextData conn ("StreamNotFound" :: B.ByteString)
        <|> if b
          then retry
          else return $ WS.sendTextData conn ("EOF" :: B.ByteString)

  forever $ do
    req <- WS.receiveData conn
    case decodeOrFail req of
      Right (_, _, r) -> join $ atomically $ transaction r
      Left (_, _, e) -> WS.sendClose conn (fromString e :: B.ByteString)

-- | The final offset.
getLastOffset :: Stream -> STM Int64
getLastOffset Stream{..} = readTVar vPayload >>= \m -> case S.viewr m of
  _ S.:> (_, p) -> return p
  S.EmptyR -> readTVar vOffsets >>= \n -> case S.viewr n of
    _ S.:> p -> return p
    S.EmptyR -> return 0

data MonotonicityViolation = MonotonicityViolation deriving Show
instance Exception MonotonicityViolation

-- | Push a payload.
pushPayload :: Stream -> B.ByteString -> STM ()
pushPayload sys@Stream{..} content = do
  p <- getLastOffset sys

  let !p' = p + fromIntegral (B.length content)
  modifyTVar' vPayload (S.|> (content, p'))

createStream :: FilePath -> StreamId -> IO Stream
createStream prefix name = do
  vPayload <- newTVarIO S.empty
  vAccess <- newTVarIO False

  let ipath = prefix </> B.unpack (BS.fromShort name) <.> "indices"
  let ppath = prefix </> B.unpack (BS.fromShort name) <.> "payload"

  vOffsets <- loadIndices ipath >>= newTVarIO
  theHandle <- openBinaryFile ppath ReadWriteMode
  hSetBuffering theHandle (BlockBuffering Nothing)

  _ <- forkIO $ synchronise ipath Stream{..}
  return Stream{..}

handleProducer :: Server -> WS.Connection -> IO ()
handleProducer Server{..} conn = forever $ do
  reqBS <- WS.receiveData conn
  case runGetOrFail get reqBS of
    Right (BL.toStrict -> !content, _, req) -> join $ atomically $ case req of
      WriteSeqNo s -> do
        ss <- readTVar vStreams
        case HM.lookup s ss of
          Just stream -> return () <$ pushPayload stream content
          Nothing -> return $ WS.sendTextData conn ("StreamNotFound" :: B.ByteString)
      NewStream s -> do
        ss <- readTVar vStreams
        return $ case HM.lookup s ss of
          Just _ -> return ()
          Nothing -> acquire vNewStream $ do
            stream <- createStream serverPrefix s
            ss1 <- atomically $ do
              ss1 <- HM.insert s stream <$> readTVar vStreams
              writeTVar vStreams ss1
              return ss1
            B.writeFile (serverPrefix </> "streams")
                $ B.unlines $ map BS.fromShort $ HM.keys ss1
    Left _ -> WS.sendClose conn ("Malformed request" :: B.ByteString)

loadIndices :: FilePath -> IO (S.Seq Int64)
loadIndices path = doesFileExist path >>= \case
  False -> return S.empty
  True -> do
    bs <- BL.readFile path
    let n = fromIntegral $ BL.length bs `div` 8
    return $! S.fromList $ runGet (replicateM n get) bs

synchronise :: FilePath -> Stream -> IO ()
synchronise ipath Stream{..} = forever $ do
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

-- | Start a liszt server. 'openLisztStream "foo"' creates two files:
--
-- * @foo.indices@: A list of offsets.
--
-- * @foo.payload@: All payloads concatenated as one file.
--
openLisztServer :: FilePath -> IO (Server, WS.ServerApp)
openLisztServer path = do
  let serverPrefix = dropTrailingPathSeparator path
  streamNames <- map BS.toShort . B.lines <$> B.readFile (serverPrefix </> "streams")
  streams <- traverse (\s -> (,) s <$> createStream serverPrefix s) streamNames
  vStreams <- newTVarIO $! HM.fromList streams
  vNewStream <- newTVarIO False
  let sys = Server{..}
  return (sys
    , \pending -> case WS.requestPath (WS.pendingRequest pending) of
    "read" -> WS.acceptRequest pending >>= handleConsumer sys
    "write" -> WS.acceptRequest pending >>= handleProducer sys
    p -> WS.rejectRequest pending ("Bad request: " <> p))
