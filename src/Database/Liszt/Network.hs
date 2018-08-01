{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Network
  ( startServer
  , Connection
  , withConnection
  , connect
  , disconnect
  , fetch) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Database.Liszt.Tracker
import Database.Liszt.Internal (hPayload, RawPointer(..))
import Data.Binary
import Data.Binary.Get
import Data.Winery
import qualified Data.Winery.Internal.Builder as WB
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.IO
import Text.Read (readMaybe)

respond :: Tracker -> S.Socket -> IO ()
respond tracker conn = do
  msg <- SB.recv conn 4096
  req <- try (evaluate $ decodeCurrent msg) >>= \case
    Left e -> throwIO $ WineryError e
    Right a -> return a
  unless (B.null msg) $ handleRequest tracker req $ \lh lastSeqNo offsets -> do
    let count = length offsets
    _ <- SB.send conn $ encodeResp $ Right count
    forM_ (zip [lastSeqNo - count + 1..] offsets) $ \(i, (tag, RP pos len)) -> do
      SB.sendAll conn $ WB.toByteString $ mconcat
        [ WB.word64 (fromIntegral i)
        , WB.word64 (fromIntegral $ WB.getSize tag), tag
        , WB.word64 $ fromIntegral len]
      SF.sendFile' conn (hPayload lh) (fromIntegral pos) (fromIntegral len)

startServer :: Int -> FilePath -> IO ()
startServer port prefix = withLisztReader prefix $ \env -> do
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
        path <- decode .  BL.fromStrict <$> SB.recv conn 4096
        withTracker env path $ \t -> do
          SB.sendAll conn $ B.pack "READY"
          forever $ respond t conn)
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encodeResp $ Left $ show (e :: LisztError)
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

encodeResp :: Either String Int -> B.ByteString
encodeResp = BL.toStrict . encode

newtype Connection = Connection (MVar S.Socket)

withConnection :: String -> Int -> B.ByteString -> (Connection -> IO r) -> IO r
withConnection host port path = bracket (connect host port path) disconnect

connect :: String -> Int -> B.ByteString -> IO Connection
connect host port path = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ BL.toStrict $ encode path
  _ <- SB.recv sock 4096
  Connection <$> newMVar sock

disconnect :: Connection -> IO ()
disconnect (Connection sock) = takeMVar sock >>= S.close

fetch :: Connection -> Request -> IO [(Int, B.ByteString, B.ByteString)]
fetch (Connection msock) req = modifyMVar msock $ \sock -> do
  SB.sendAll sock $ serialiseOnly req
  go sock $ runGetIncremental $ get >>= \case
    Left e -> case readMaybe e of
      Just e' -> throw (e' :: LisztError)
      Nothing -> fail $ "Unknown error: " ++ show e
    Right n -> replicateM n ((,,) <$> get <*> get <*> get)
  where
    go sock (Done _ _ a) = return (sock, a)
    go sock (Partial cont) = do
      bs <- SB.recv sock 4096
      if B.null bs then go sock $ cont Nothing else go sock $ cont $ Just bs
    go _ (Fail _ _ str) = fail $ show req ++ ": " ++ str
