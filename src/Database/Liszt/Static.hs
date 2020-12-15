module Database.Liszt.Static (StaticLiszt, openStaticLiszt) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import Database.Liszt.Internal
import Foreign.Ptr
import Foreign.ForeignPtr
import System.IO.MMap

newtype StaticLiszt = StaticLiszt B.ByteString

instance Fetchable StaticLiszt where
  fetchNode (StaticLiszt (B.PS fp ofs _)) (RP pos _) = withForeignPtr fp
    $ \ptr -> peekNode (ptr `plusPtr` ofs `plusPtr` pos)
  fetchKey (StaticLiszt bs) (KeyPointer (RP pos len)) = return
    $! Key $ B.take len $ B.drop pos bs
  fetchRoot s@(StaticLiszt bs) = fetchNode s (RP (B.length bs - footerSize) footerSize)
  fetchPayload (StaticLiszt bs) (RP pos len) = return $! B.take len $ B.drop pos bs

openStaticLiszt :: FilePath -> IO StaticLiszt
openStaticLiszt path = StaticLiszt <$> mmapFileByteString path Nothing
