{-# LANGUAGE LambdaCase, OverloadedStrings #-}
module Database.Liszt.Network
  ( startServer
  , Connection
  , withConnection
  , connect
  , disconnect
  , fetch) where

import Control.Concurrent
import Control.Exception (evaluate, throw, throwIO)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Debug.Trace
import Database.Liszt.Tracker
import Database.Liszt.Internal (hPayload, RawPointer(..))
import Data.Serialize
import Data.Serialize.Get as S
import qualified Data.ByteString.Char8 as B
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import qualified Mason.Builder as BB
import Codec.Winery
import Codec.Winery.Internal
import System.FilePath ((</>))
import System.IO

respond :: Tracker -> S.Socket -> IO ()
respond tracker conn = do
  msg <- SB.recv conn 4096
  unless (B.null msg) $ do
    req <- case deserialise msg of
      Left e -> throwIO $ WineryError e
      Right a -> return a
    handleRequest tracker req $ \lh lastSeqNo offsets -> do
      let count = length offsets
      _ <- SB.send conn $ encodeResp $ Right count
      forM_ (zip [lastSeqNo - count + 1..] offsets) $ \(i, (tag, RP pos len)) -> do
        BB.sendBuilder conn $ mconcat
          [ BB.word64LE (fromIntegral i)
          , BB.word64LE (fromIntegral $ B.length tag), BB.byteString tag
          , BB.word64LE $ fromIntegral len]
        SF.sendFile' conn (hPayload lh) (fromIntegral pos) (fromIntegral len)
    respond tracker conn

startServer :: Int -> FilePath -> IO ()
startServer port prefix = withLisztReader $ \env -> do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock 2
    forever $ do
      (conn, _) <- S.accept sock
      forkFinally (do
        bs <- SB.recv conn 4096
        path <- case decode bs of
          Right a -> return a
          Left _ -> throwM InvalidRequest
        withTracker env (prefix </> path) $ \t -> do
          SB.sendAll conn $ B.pack "READY"
          respond t conn)
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encodeResp $ Left $ show (e :: LisztError)
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

encodeResp :: Either String Int -> B.ByteString
encodeResp = encode

newtype Connection = Connection (MVar S.Socket)

withConnection :: (MonadIO m, MonadMask m) => String -> Int -> B.ByteString -> (Connection -> m r) -> m r
withConnection host port path = bracket (connect host port path) disconnect

connect :: MonadIO m => String -> Int -> B.ByteString -> m Connection
connect host port path = liftIO $ do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ encode path
  resp <- SB.recv sock 4096
  case resp of
    "READY" -> Connection <$> newMVar sock
    e -> fail $ "connect: Unexpected response: " ++ show e

disconnect :: MonadIO m => Connection -> m ()
disconnect (Connection sock) = liftIO $ takeMVar sock >>= S.close

fetch :: MonadIO m => Connection -> Request -> m [(Int, B.ByteString, B.ByteString)]
fetch (Connection msock) req = liftIO $ modifyMVar msock $ \sock -> do
  SB.sendAll sock $ serialise req
  bs <- SB.recv sock 4096
  go sock $ flip runGetPartial bs $ get >>= \case
    Left e -> fail $ "Unknown error: " ++ e
    Right n -> replicateM n $ do
	i <- getWord64le
        tagLen <- fromIntegral <$> getWord64le
        tag <- S.getBytes tagLen
        payloadLen <- fromIntegral <$> getWord64le
        payload <- S.getBytes payloadLen
	pure (fromIntegral i, tag, payload)
  where
    go sock (Done a _) = return (sock, a)
    go sock (Partial cont) = do
      bs <- SB.recv sock 4096
      if B.null bs then go sock $ cont "" else go sock $ cont bs
    go _ (Fail str _) = fail $ show req ++ ": " ++ str
