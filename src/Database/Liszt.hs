{-# LANGUAGE DeriveGeneric, RecordWildCards, LambdaCase, Rank2Types, ScopedTypeVariables #-}
module Database.Liszt (
    -- * Writer interface
    Transaction,
    insert,
    commit,
    commitFile,
    -- * Reader
    Request(..),
    RequestType(..),
    defRequest,
    IndexMap,
    LisztError(..),
    LisztReader,
    withLisztReader,
    handleRequest,
    fetchLocal,
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Database.Liszt.Internal
import Data.Binary
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import Data.Foldable (toList)
import Data.Functor.Identity
import qualified Data.HashMap.Strict as HM
import Data.Int
import qualified Data.IntMap.Strict as IM
import Data.Maybe (isJust)
import Data.Proxy
import Database.Liszt.Internal
import GHC.Generics (Generic)
import System.Directory
import System.FilePath
import System.IO
import System.FSNotify

commitFile :: FilePath -> Transaction () -> IO ()
commitFile path m = withBinaryFile path ReadWriteMode $ \h -> commit h m

data RequestType = AllItems | LastItem deriving (Show, Generic)
instance Binary RequestType

data Request = Request
  { streamName :: !B.ByteString
  , reqFromIndex :: !(Maybe B.ByteString)
  , reqToIndex :: !(Maybe B.ByteString)
  , reqTimeout :: !Int
  , reqType :: !RequestType
  , reqFrom :: !Int
  , reqTo :: !Int
  } deriving (Show, Generic)
instance Binary Request

defRequest :: B.ByteString -> Request
defRequest name = Request
  { streamName = name
  , reqFromIndex = Nothing
  , reqToIndex = Nothing
  , reqTimeout = maxBound `div` 2
  , reqFrom = 0
  , reqTo = 0
  , reqType = AllItems
  }

type IndexMap = HM.HashMap B.ByteString

data Stream = Stream
  { vRoot :: TMVar (Frame RawPointer)
  , followThread :: ThreadId
  , streamHandle :: Handle
  }

createStream :: WatchManager -> FilePath -> IO Stream
createStream man path = undefined {-do
  exist <- doesFileExist path
  unless exist $ throwIO StreamNotFound
  payloadHandle <- openBinaryFile path ReadMode
  vCaughtUp <- newTVarIO False
  watchDir man path (\case
    Modified path _ _ | path == offsetPath -> True
    _ -> False)
    $ const $ atomically $ takeTMVar vRoot

  followThread <- forkIO $ do
    hSeek h SeekFromEnd 2048
    forever $ do
      bs <- B.hGet h 2048
      when (B.length bs == 2048) $ try (evaluate (decodeFrame bs)) >>= \case
        Left _ -> return ()
        Right a -> do
          atomically $ putTMVar vRoot a
          atomically $ isEmptyTMVar vRoot >>= \b -> unless b retry

  return Stream{..} -}

data LisztError = MalformedRequest
  | StreamNotFound
  | IndexNotFound
  deriving Show
instance Exception LisztError

data LisztReader = LisztReader
  { watchManager :: WatchManager
  , vStreams :: TVar (HM.HashMap B.ByteString Stream)
  , prefix :: FilePath
  }

withLisztReader :: FilePath -> (LisztReader -> IO ()) -> IO ()
withLisztReader prefix k = do
  vStreams <- newTVarIO HM.empty
  withManager $ \watchManager -> k LisztReader{..}

handleRequest :: LisztReader
  -> Request
  -> IO (Handle, [(Int, IndexMap Int, Int, Int)])
handleRequest _ _ = undefined

fetchLocal :: LisztReader -> Request -> IO [(Int, IndexMap Int, B.ByteString)]
fetchLocal env req = undefined
