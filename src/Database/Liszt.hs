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
    fetchRange,
    -- * Remote reader
    Offset(..),
    Request(..),
    defRequest,
    Connection,
    withConnection,
    fetch,
    LisztError(..)
    ) where

import Control.Monad.Catch
import Control.Monad.IO.Class
import Database.Liszt.Internal
import Database.Liszt.Network
import Database.Liszt.Tracker
import Data.Winery

-- | Commit a 'Transaction' to a file.
commitFile :: (MonadIO m, MonadMask m) => FilePath -> Transaction a -> m a
commitFile path m = withLiszt path $ \h -> commit h m

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

fetchRange :: MonadIO m => LisztHandle -> Key -> Int -> Int -> m [(Int, Tag, RawPointer)]
fetchRange h key i j = liftIO $ do
  root <- fetchRoot h
  lookupSpine h key root >>= \case
    Nothing -> return []
    Just spine -> do
      let len = spineLength spine
      spine' <- dropSpine h ((-i) `mod` len) spine
      result <- takeSpine h (mod j len - mod i len + 1) spine' []
      return [(k, t, rp) | (k, (t, rp)) <- zip [i `mod` len ..] result]
