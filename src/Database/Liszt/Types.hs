{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Types where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int
import Data.ByteString.Short (ShortByteString)

type StreamId = ShortByteString
type IndexId = ShortByteString

data Offset = Offset !Int64
    | Index !IndexId !Int64
    deriving Show

getChar8 :: Get Char
getChar8 = toEnum . fromEnum <$> getWord8

putChar8 :: Char -> Put
putChar8 = putWord8 . toEnum . fromEnum

instance Binary Offset where
  get = getChar8 >>= \case
    'O' -> Offset <$> get
    'I' -> Index <$> get <*> get
  put (Offset o) = putChar8 'O' >> put o
  put (Index i j) = putChar8 'I' >> put i >> put j

data ConsumerRequest = Read !StreamId !Offset !Bool deriving Show

instance Binary ConsumerRequest where
  get = getChar8 >>= \case
    'R' -> Read <$> get <*> get <*> get
    _ -> fail "Unknown tag"
  put (Read s o b) = putChar8 'R' >> put s >> put o >> put b

data ProducerRequest = WriteSeqNo !StreamId
  | NewStream !StreamId
  deriving Show

instance Binary ProducerRequest where
  get = getChar8 >>= \case
    'S' -> WriteSeqNo <$> get
    _ -> fail "Unknown tag"
  put (WriteSeqNo s) = putChar8 'S' >> put s
