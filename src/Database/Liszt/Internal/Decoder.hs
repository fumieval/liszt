module Database.Liszt.Internal.Decoder (Decoder(..)
  , runDecoder
  , DecodeResult(..)
  , decodeWord8
  , decodeVarInt
  , decodeInt) where

import Control.Monad.Catch
import Foreign.Ptr
import Foreign.Storable
import Data.Bits
import Data.Word
import System.Endian

newtype Decoder a = Decoder { unDecoder :: Ptr Word8 -> IO (DecodeResult a) }

runDecoder :: Decoder a -> Ptr Word8 -> IO a
runDecoder m p = do
  DecodeResult _ a <- unDecoder m p
  return a

data DecodeResult a = DecodeResult {-# UNPACK #-} !(Ptr Word8) !a

instance MonadThrow Decoder where
  throwM e = Decoder $ \_ -> throwM e

instance Functor Decoder where
  fmap f m = Decoder $ \p -> do
    DecodeResult p' a <- unDecoder m p
    return (DecodeResult p' (f a))
  {-# INLINE fmap #-}

instance Applicative Decoder where
  pure a = Decoder $ \p -> pure $ DecodeResult p a
  {-# INLINE pure #-}
  m <*> n = Decoder $ \p -> do
    DecodeResult p' f <- unDecoder m p
    DecodeResult p'' a <- unDecoder n p'
    return $ DecodeResult p'' (f a)
  {-# INLINE (<*>) #-}

instance Monad Decoder where
  return = pure
  m >>= k = Decoder $ \p -> do
    DecodeResult p' a <- unDecoder m p
    unDecoder (k a) p'
  {-# INLINE (>>=) #-}
  (>>) = (*>)

decodeWord8 :: Decoder Word8
decodeWord8 = Decoder $ \ptr -> do
  a <- peek ptr
  return $ DecodeResult (ptr `plusPtr` 1) a
{-# INLINE decodeWord8 #-}

decodeVarInt :: Decoder Int
decodeVarInt = decodeWord8 >>= go
  where
    go n
      | testBit n 7 = do
        m <- decodeWord8 >>= go
        return $! unsafeShiftL m 7 .|. clearBit (fromIntegral n) 7
      | otherwise = return $ fromIntegral n
{-# INLINE decodeVarInt #-}

decodeInt :: Decoder Int
decodeInt = Decoder $ \ptr -> do
  a <- peek (castPtr ptr)
  return $ DecodeResult (ptr `plusPtr` 8) (fromIntegral $ fromBE64 a)
{-# INLINE decodeInt #-}
