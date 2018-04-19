{-# LANGUAGE RecordWildCards, LambdaCase, OverloadedStrings, ViewPatterns, BangPatterns #-}
module Database.Liszt.Server (Server
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
import Data.Int
import Data.List (foldl')
import Data.Semigroup
import Data.String
import Database.Liszt.Types
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as M
import qualified Network.WebSockets as WS
import System.Directory
import System.IO

data Server = Server
    { vPayload :: TVar (M.IntMap (B.ByteString, Int64))
    -- ^ A collection of payloads which are not available on disk.
    , vOffsets :: TVar (M.IntMap Int64)
    -- ^ Map of byte offsets
    , vIndices :: TVar (HM.HashMap IndexName (M.IntMap M.Key))
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
fetchPayload :: Server
  -> TVar (M.Key, Int64)
  -> STM (IO (M.Key, B.ByteString))
fetchPayload Server{..} v = do
  (ofs, pos) <- readTVar v
  m <- readTVar vOffsets
  case M.lookupGT ofs m of
    Just op@(_, pos') -> do
      writeTVar v op
      return $ acquire vAccess $ do
        hSeek theHandle AbsoluteSeek (fromIntegral pos)
        bs <- B.hGet theHandle $ fromIntegral $ pos' - pos
        return (ofs, bs)
    Nothing -> M.lookupGT ofs <$> readTVar vPayload >>= \case
      Just (ofs', (bs, pos')) -> do
        writeTVar v (ofs', pos')
        return $ return (ofs, bs)
      Nothing -> retry

handleConsumer :: Server -> WS.Connection -> IO ()
handleConsumer sys@Server{..} conn = do
  -- start from the beginning of the stream
  vOffset <- newTVarIO (minBound, 0)

  let transaction (NonBlocking r) = transaction r
        <|> pure (WS.sendTextData conn ("EOF" :: BL.ByteString))
      transaction Read = (>>= \(i, bs) -> WS.sendBinaryData conn $ B.runPut $ B.put i >> B.putByteString bs )
          <$> fetchPayload sys vOffset
      transaction Peek = WS.sendBinaryData conn . encode . fst
          <$> readTVar vOffset
      transaction (Seek ofs) = do
        m <- readTVar vOffsets
        writeTVar vOffset $ maybe (minBound, 0) id $ M.lookupLT (fromIntegral ofs) m
        return $ return ()

  forever $ do
    req <- WS.receiveData conn
    case decodeOrFail req of
      Right (_, _, r) -> join $ atomically $ transaction r
      Left (_, _, e) -> WS.sendClose conn (fromString e :: B.ByteString)

-- | The final offset.
getLastOffset :: Server -> STM (Maybe (M.Key, Int64))
getLastOffset Server{..} = readTVar vPayload >>= \m -> case M.maxViewWithKey m of
  Just ((k, (_, p)), _) -> return $ Just (k, p)
  Nothing -> fmap fst <$> M.maxViewWithKey <$> readTVar vOffsets

data MonotonicityViolation = MonotonicityViolation deriving Show
instance Exception MonotonicityViolation

-- | Push a payload.
pushPayload :: Server -> [(IndexName, M.Key)] -> B.ByteString -> STM ()
pushPayload sys@Server{..} ks content = do
  (o, p) <- getLastOffset sys >>= \case
    Just (o0, p) -> return (o0 + 1, p)
    Nothing -> return (0, 0)

  let !p' = p + fromIntegral (B.length content)
  modifyTVar' vPayload (M.insert o (content, p'))
  modifyTVar' vIndices
    $ \m0 -> foldl' (\m (name, i) -> HM.insertWith M.union name (M.singleton i o) m) m0 ks

handleProducer :: Server -> WS.Connection -> IO ()
handleProducer sys@Server{..} conn = forever $ do
  reqBS <- WS.receiveData conn
  case runGetOrFail get reqBS of
    Right (BL.toStrict -> !content, _, req) -> join $ atomically $ do
      let push k p = return () <$ pushPayload sys k p

      case req of
        Write k -> push (fmap fromIntegral <$> k) content
          `catchSTM` \MonotonicityViolation -> return
              (WS.sendClose conn ("Monotonicity violation" :: B.ByteString))
        WriteSeqNo -> push [] content

    Left _ -> WS.sendClose conn ("Malformed request" :: B.ByteString)

loadIndices :: FilePath -> IO (M.IntMap Int64)
loadIndices path = doesFileExist path >>= \case
  False -> return M.empty
  True -> do
    n <- (`div`16) <$> fromIntegral <$> getFileSize path
    bs <- BL.readFile path
    return $! M.fromAscList $ runGet (replicateM n $ (,) <$> get <*> get) bs

synchronise :: FilePath -> Server -> IO ()
synchronise ipath Server{..} = forever $ do
  m <- atomically $ do
    w <- readTVar vPayload
    when (M.null w) retry
    return w

  acquire vAccess $ do
    hSeek theHandle SeekFromEnd 0
    mapM_ (B.hPut theHandle . fst) m
    hFlush theHandle

  BL.appendFile ipath
    $ B.runPut $ forM_ (M.toList m) $ \(k, (_, p)) -> B.put k >> B.put p

  atomically $ do
    modifyTVar' vOffsets $ M.union (fmap snd m)
    modifyTVar' vPayload $ flip M.difference m

-- | Start a liszt server. 'openLisztServer "foo"' creates two files:
--
-- * @foo.indices@: A list of offsets.
--
-- * @foo.payload@: All payloads concatenated as one file.
--
openLisztServer :: FilePath -> IO (Server, ThreadId, WS.ServerApp)
openLisztServer path = do
  let ipath = path ++ ".offsets"
  let ppath = path ++ ".payload"

  vOffsets <- loadIndices ipath >>= newTVarIO
  vIndices <- newTVarIO HM.empty

  vPayload <- newTVarIO M.empty

  vAccess <- newTVarIO False

  theHandle <- openBinaryFile ppath ReadWriteMode
  hSetBuffering theHandle (BlockBuffering Nothing)

  let sys = Server{..}

  tid <- forkIO $ forever $ synchronise ipath sys
    `catch` \e -> hPutStrLn stderr $ "synchronise: " ++ show (e :: IOException)

  return (sys, tid, \pending -> case WS.requestPath (WS.pendingRequest pending) of
    "read" -> WS.acceptRequest pending >>= handleConsumer sys
    "write" -> WS.acceptRequest pending >>= handleProducer sys
    p -> WS.rejectRequest pending ("Bad request: " <> p))
