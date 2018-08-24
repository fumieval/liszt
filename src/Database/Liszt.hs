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
    -- * Reader
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
