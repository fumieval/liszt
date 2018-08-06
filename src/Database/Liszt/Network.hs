{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Network
  (startServer
  , Connection
  , withConnection
  , connect
  , disconnect
  , fetch) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Database.Liszt
import Database.Liszt.Internal
import Data.Binary
import Data.Binary.Get
import Data.Winery
import qualified Data.Winery.Internal.Builder as WB
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.IO

respond :: LisztReader -> S.Socket -> IO ()
respond env conn = do
  msg <- SB.recv conn 4096
  req <- try (evaluate $ decodeCurrent msg) >>= \case
    Left e -> throwIO $ WineryError e
    Right a -> return a
  unless (B.null msg) $ handleRequest env req $ \lh lastSeqNo offsets -> do
    let count = length offsets
    SB.sendAll conn $ encodeResp $ Right count
    forM_ (zip [lastSeqNo - count + 1..] offsets) $ \(i, (tag, RP pos len)) -> do
      SB.sendAll conn $ WB.toByteString $ mconcat
        [ WB.word64 (fromIntegral i)
        , WB.word64 (fromIntegral $ WB.getSize tag), tag
        , WB.word64 $ fromIntegral len]
      SF.sendFile' conn (hPayload lh) (fromIntegral pos) (fromIntegral len)

startServer :: Int -> FilePath -> IO ()
startServer port path = withLisztReader path $ \env -> do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock 2
    forever $ do
      (conn, _) <- S.accept sock
      forkFinally (forever $ respond env conn)
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encodeResp $ Left $ show (e :: LisztError)
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

encodeResp :: Either String Int -> B.ByteString
encodeResp = BL.toStrict . encode

newtype Connection = Connection S.Socket

withConnection :: String -> Int -> (Connection -> IO r) -> IO r
withConnection host port = bracket (connect host port) disconnect

connect :: String -> Int -> IO Connection
connect host port = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  return $ Connection sock

disconnect :: Connection -> IO ()
disconnect (Connection sock) = S.close sock

fetch :: Connection -> Request -> IO [(Int, B.ByteString, B.ByteString)]
fetch (Connection sock) req = do
  SB.sendAll sock $ serialiseOnly req
  go $ runGetIncremental $ get >>= \case
    Left e -> throw (read e :: LisztError)
    Right n -> replicateM n ((,,) <$> get <*> get <*> get)
  where
    go (Done _ _ a) = return a
    go (Partial cont) = do
      bs <- SB.recv sock 4096
      if B.null bs then go $ cont Nothing else go $ cont $ Just bs
    go (Fail _ _ str) = fail $ show req ++ ": " ++ str
