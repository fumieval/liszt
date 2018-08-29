{-# LANGUAGE LambdaCase #-}
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
    insertTagged,
    insertRaw,
    commit,
    commitFile,
    -- * Local reader
    RawPointer,
    count,
    fetchRange,
    fetchPayload,
    -- * Remote reader
    Offset(..),
    Request(..),
    defRequest,
    Connection,
    withConnection,
    fetch,
    LisztError(..)
    ) where

import Control.Monad.IO.Class
import Database.Liszt.Internal
import Database.Liszt.Network
import Database.Liszt.Tracker
import Data.Winery

-- | Commit a 'Transaction' to a file.
commitFile :: (MonadIO m) => FilePath -> Transaction a -> m a
commitFile path m = liftIO $ withLiszt path $ \h -> commit h m

-- | Insert a value.
insert :: Serialise a => Key -> a -> Transaction ()
insert k v = insertRaw k mempty (toEncoding v)
{-# INLINE insert #-}

-- | Insert a value with a tag (e.g. timestamp).
-- Tags can be used to perform `WineryTag` query.
-- Tag values should be monotonically increasing but this is not checked.
insertTagged :: (Serialise t, Serialise a) => Key -> t -> a -> Transaction ()
insertTagged k t v = insertRaw k (toEncoding t) (toEncoding v)
{-# INLINE insertTagged #-}

-- | The number of entries in the stream
count :: MonadIO m => LisztHandle -> Key -> m Int
count h k = liftIO $ do
  root <- fetchRoot h
  maybe 0 spineLength <$> lookupSpine h k root

fetchRange :: MonadIO m => LisztHandle -> Key -> Int -> Int -> m [(Int, Tag, RawPointer)]
fetchRange h key i_ j_ = liftIO $ do
  root <- fetchRoot h
  lookupSpine h key root >>= \case
    Nothing -> return []
    Just spine -> do
      let len = spineLength spine
      let normalise x
            | x < 0 = min (len - 1) $ max 0 $ len + x
            | otherwise = min (len - 1) x
      let j = normalise j_
      let i = normalise i_
      spine' <- dropSpine h (len - j - 1) spine
      result <- takeSpine h (j - i + 1) spine' []
      return [(k, t, rp) | (k, (t, rp)) <- zip [i..] result]
