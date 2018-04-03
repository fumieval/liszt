{-# LANGUAGE LambdaCase, OverloadedStrings #-}
module Database.Liszt.Client (
  -- * Consumer
  Consumer
  , withConsumer
  , readBlocking
  , readNonBlocking
  -- * Producer
  , Producer
  , withProducer
  , writeSeqNo
  ) where

import Control.Exception
import Control.Monad.IO.Class
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Database.Liszt.Types
import Network.WebSockets

-- | Connection as a consumer
newtype Consumer = Consumer Connection

-- | Acquire a consumer.
withConsumer :: String -> Int -> String -> (Consumer -> IO a) -> IO a
withConsumer host port name k = runClient host port ("/" ++ name ++ "/read") $ k . Consumer

parsePayload :: MonadIO m => BL.ByteString -> m (Int64, B.ByteString)
parsePayload bs = liftIO $ case runGetOrFail get bs of
  Right (content, _, ofs) -> return (ofs, BL.toStrict content)
  Left _ -> throwIO $ ParseException "Malformed response"

-- | Fetch a payload.
readBlocking :: MonadIO m => Consumer -> StreamId -> Offset -> m (Int64, B.ByteString)
readBlocking (Consumer conn) name ofs = liftIO $ do
  sendBinaryData conn $ encode $ Read name ofs True
  receiveData conn >>= parsePayload

-- | Fetch a payload.
readNonBlocking :: MonadIO m => Consumer -> StreamId -> Offset -> m (Maybe (Int64, B.ByteString))
readNonBlocking (Consumer conn) name ofs = liftIO $ do
  sendBinaryData conn $ encode $ Read name ofs False
  receiveDataMessage conn >>= \case
    Text "EOF" _ -> return Nothing
    Binary bs -> Just <$> parsePayload bs
    _ -> throwIO $ ParseException "Expecting EOF"

-- | Connection as a producer
newtype Producer = Producer Connection

-- | Acquire a producer.
withProducer :: String -> Int -> String -> (Producer -> IO a) -> IO a
withProducer host port name k = runClient host port ("/" ++ name ++ "/write") $ k . Producer

-- | Write a payload with an increasing natural number as an offset (starts from 0).
-- Atomic and non-blocking.
writeSeqNo :: MonadIO m => Producer -> StreamId -> B.ByteString -> m ()
writeSeqNo (Producer conn) name bs = liftIO $ sendBinaryData conn $ runPut $ do
  put $ WriteSeqNo name
  putByteString bs
