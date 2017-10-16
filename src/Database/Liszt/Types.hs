{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Types where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int

data ConsumerRequest = Read
  | Peek
  | Seek !Int64
  | NonBlocking ConsumerRequest deriving Show

instance Binary ConsumerRequest where
  get = getWord8 >>= \case
    78{-N-} -> NonBlocking <$> get
    80{-P-} -> pure Peek
    82{-R-} -> pure Read
    83{-S-} -> Seek <$> get
    _ -> fail "Unknown tag"
  put Peek = putWord8 80
  put Read = putWord8 82
  put (Seek b) = putWord8 83 >> put b
  put (NonBlocking r) = putWord8 78 >> put r

data ProducerRequest = WriteSeqNo
  deriving Show

instance Binary ProducerRequest where
  get = getWord8 >>= \case
    83{-S-} -> pure WriteSeqNo
    _ -> fail "Unknown tag"
  put WriteSeqNo = putWord8 83
