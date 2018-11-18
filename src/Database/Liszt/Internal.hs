{-# LANGUAGE LambdaCase, DeriveTraversable, DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
module Database.Liszt.Internal (
  Key
  , Tag
  , LisztHandle(..)
  , openLiszt
  , closeLiszt
  , withLiszt
  -- * Writing
  , clear
  , insertRaw
  , commit
  , Transaction
  , TransactionState
  -- * Reading
  , availableKeys
  -- * Node
  , Node(..)
  , peekNode
  , LisztDecodingException(..)
  -- * Fetching
  , Fetchable(..)
  , KeyPointer(..)
  , RawPointer(..)
  , fetchPayload
  -- * Footer
  , footerSize
  , isFooter
  -- * Node
  , lookupSpine
  , traverseNode
  -- * Spine
  , Spine
  , spineLength
  , QueryResult
  , wholeSpine
  , takeSpine
  , dropSpine
  , takeSpineWhile
  , dropSpineWhile
  ) where

import Control.Concurrent
import Control.DeepSeq
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Data.Bifunctor
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import qualified Data.IntMap.Strict as IM
import Data.IORef
import Data.Monoid
import Data.Word
import Data.Winery.Internal.Builder
import Database.Liszt.Internal.Decoder
import Foreign.ForeignPtr
import Foreign.Ptr
import GHC.Generics (Generic)
import System.Directory
import System.IO

type Key = B.ByteString

-- | Tag is an extra value attached to a payload. This can be used to perform
-- a binary search.
type Tag = B.ByteString

type Spine a = [(Int, a)]

newtype KeyPointer = KeyPointer RawPointer deriving (Show, Eq, NFData)

data RawPointer = RP !Int !Int deriving (Show, Eq, Generic)
instance NFData RawPointer where
  rnf r = r `seq` ()

data Node t a = Empty
  | Leaf1 !KeyPointer !(Spine a)
  | Leaf2 !KeyPointer !(Spine a) !KeyPointer !(Spine a)
  | Node2 !a !KeyPointer !(Spine a) !a
  | Node3 !a !KeyPointer !(Spine a) !a !KeyPointer !(Spine a) !a
  | Tip !t !RawPointer
  | Bin !t !RawPointer !a !a
  deriving (Generic, Show, Eq, Functor, Foldable, Traversable)

encodeNode :: Node Encoding RawPointer -> Encoding
encodeNode Empty = word8 0x00
encodeNode (Leaf1 (KeyPointer p) s) = word8 0x01 <> encodeRP p <> encodeSpine s
encodeNode (Leaf2 (KeyPointer p) s (KeyPointer q) t) = word8 0x02
  <> encodeRP p <> encodeSpine s <> encodeRP q <> encodeSpine t
encodeNode (Node2 l (KeyPointer p) s r) = word8 0x12
  <> encodeRP l <> encodeRP p <> encodeSpine s <> encodeRP r
encodeNode (Node3 l (KeyPointer p) s m (KeyPointer q) t r) = word8 0x13
  <> encodeRP l <> encodeRP p <> encodeSpine s
  <> encodeRP m <> encodeRP q <> encodeSpine t <> encodeRP r
encodeNode (Tip t p) = word8 0x80 <> unsignedVarInt (getSize t) <> t <> encodeRP p
encodeNode (Bin t p l r) = word8 0x81 <> unsignedVarInt (getSize t) <> t
  <> encodeRP p <> encodeRP l <> encodeRP r

encodeRP :: RawPointer -> Encoding
encodeRP (RP p l) = unsignedVarInt p <> unsignedVarInt l

encodeSpine :: Spine RawPointer -> Encoding
encodeSpine s = unsignedVarInt (length s) <> foldMap (\(r, p) -> unsignedVarInt r <> encodeRP p) s

data LisztDecodingException = LisztDecodingException deriving Show
instance Exception LisztDecodingException

peekNode :: Ptr Word8 -> IO (Node Tag RawPointer)
peekNode = runDecoder $ decodeWord8 >>= \case
  0x00 -> return Empty
  0x01 -> Leaf1 <$> kp <*> spine
  0x02 -> Leaf2 <$> kp <*> spine <*> kp <*> spine
  0x12 -> Node2 <$> rp <*> kp <*> spine <*> rp
  0x13 -> Node3 <$> rp <*> kp <*> spine <*> rp <*> kp <*> spine <*> rp
  0x80 -> Tip <$> bs <*> rp
  0x81 -> Bin <$> bs <*> rp <*> rp <*> rp
  _ -> throwM LisztDecodingException
  where
    kp = fmap KeyPointer rp
    rp = RP <$> decodeVarInt <*> decodeVarInt
    spine = do
      len <- decodeVarInt
      replicateM len $ (,) <$> decodeVarInt <*> rp
    bs = do
      len <- decodeVarInt
      Decoder $ \src -> do
        fp <- B.mallocByteString len
        withForeignPtr fp $ \dst -> B.memcpy dst src len
        return $ DecodeResult (src `plusPtr` len) (B.PS fp 0 len)

instance Bifunctor Node where
  bimap f g = \case
    Empty -> Empty
    Leaf1 k s -> Leaf1 k (map (fmap g) s)
    Leaf2 j s k t -> Leaf2 j (map (fmap g) s) k (map (fmap g) t)
    Node2 l k s r -> Node2 (g l) k (map (fmap g) s) (g r)
    Node3 l j s m k t r -> Node3 (g l) j (map (fmap g) s) (g m) k (map (fmap g) t) (g r)
    Bin t p l r -> Bin (f t) p (g l) (g r)
    Tip t p -> Tip (f t) p

data LisztHandle = LisztHandle
  { hPayload :: !Handle
  , refBuffer :: MVar (Int, ForeignPtr Word8)
  , keyCache :: IORef (IM.IntMap Key)
  , refModified :: IORef Bool
  , handleLock :: MVar ()
  }

openLiszt :: MonadIO m => FilePath -> m LisztHandle
openLiszt path = liftIO $ do
  exist <- doesFileExist path
  hPayload <- openBinaryFile path ReadWriteMode
  unless exist $ B.hPutStr hPayload emptyFooter
  buf <- B.mallocByteString 4096
  refBuffer <- newMVar (4096, buf)
  keyCache <- newIORef IM.empty
  handleLock <- newMVar ()
  refModified <- newIORef False
  return LisztHandle{..}

closeLiszt :: MonadIO m => LisztHandle -> m ()
closeLiszt lh = liftIO $ hClose $ hPayload lh

withLiszt :: (MonadIO m, MonadMask m) => FilePath -> (LisztHandle -> m a) -> m a
withLiszt path = bracket (openLiszt path) closeLiszt

--------------------------------------------------------------------------------
-- Transaction

data StagePointer = Commited !RawPointer | Uncommited !Int deriving Eq

data TransactionState = TS
  { dbHandle :: !LisztHandle
  , freshId :: !Int
  , pending :: !(IM.IntMap (Node Encoding StagePointer))
  , currentRoot :: !(Node Encoding StagePointer)
  , currentPos :: !Int
  }

type Transaction = StateT TransactionState IO

-- | Replace the specified key with an empty list.
clear :: Key -> Transaction ()
clear key = do
  root <- gets currentRoot
  insertF key (const $ return []) root >>= \case
    Pure t -> modify $ \ts -> ts { currentRoot = t }
    Carry l k a r -> modify $ \ts -> ts { currentRoot = Node2 l k a r }

append :: LisztHandle -> (Handle -> IO a) -> IO a
append LisztHandle{..} cont = do
  hSeek hPayload SeekFromEnd 0
  writeIORef refModified True
  cont hPayload

insertRaw :: Key -> Encoding -> Encoding -> Transaction ()
insertRaw key tag payload = do
  lh <- gets dbHandle
  ofs <- gets currentPos
  liftIO $ append lh $ \h -> hPutEncoding h payload
  root <- gets currentRoot
  modify $ \ts -> ts { currentPos = ofs + getSize payload }
  insertF key (insertSpine tag (ofs `RP` getSize payload)) root >>= \case
    Pure t -> modify $ \ts -> ts { currentRoot = t }
    Carry l k a r -> modify $ \ts -> ts { currentRoot = Node2 l k a r }

addNode :: Node Encoding StagePointer -> Transaction StagePointer
addNode f = state $ \ts -> (Uncommited (freshId ts), ts
  { freshId = freshId ts + 1
  , pending = IM.insert (freshId ts) f (pending ts) })

allocKey :: Key -> Transaction KeyPointer
allocKey key = do
  lh <- gets dbHandle
  ofs <- gets currentPos
  liftIO $ append lh $ \h -> do
    B.hPutStr h key
    modifyIORef' (keyCache lh) (IM.insert ofs key)
  modify $ \ts -> ts { currentPos = ofs + B.length key }
  return $ KeyPointer $ RP ofs (B.length key)

commit :: MonadIO m => LisztHandle -> Transaction a -> m a
commit h transaction = liftIO $ modifyMVar (handleLock h) $ const $ do
  offset0 <- fromIntegral <$> hFileSize h'
  root <- fetchRoot h
  do
    (a, TS _ _ pendings root' offset1) <- runStateT transaction
      $ TS h 0 IM.empty (bimap bytes Commited root) offset0

    let substP (Commited ofs) = return ofs
        substP (Uncommited i) = case IM.lookup i pendings of
          Just f -> substF f
          Nothing -> error "panic!"
        substF Empty = return (RP 0 0)
        substF (Leaf1 pk pv) = do
          pv' <- traverse (traverse substP) pv
          write (Leaf1 pk pv')
        substF (Leaf2 pk pv qk qv) = do
          pv' <- traverse (traverse substP) pv
          qv' <- traverse (traverse substP) qv
          write (Leaf2 pk pv' qk qv')
        substF (Node2 l pk pv r) = do
          l' <- substP l
          pv' <- traverse (traverse substP) pv
          r' <- substP r
          write (Node2 l' pk pv' r')
        substF (Node3 l pk pv m qk qv r) = do
          l' <- substP l
          pv' <- traverse (traverse substP) pv
          m' <- substP m
          qv' <- traverse (traverse substP) qv
          r' <- substP r
          write (Node3 l' pk pv' m' qk qv' r')
        substF (Bin t p l r) = do
          l' <- substP l
          r' <- substP r
          write (Bin t p l' r')
        substF (Tip t p) = write (Tip t p)

    writeFooter offset1 $ substF root'
    return ((), a)
    `onException` writeFooter offset0 (write $ first bytes root)
  where
    writeFooter ofs m = do
      modified <- readIORef (refModified h)
      when modified $ do
        hSeek h' SeekFromEnd 0
        RP _ len <- evalStateT m ofs
        B.hPutStr h' $ B.drop len emptyFooter
        hFlush h'
        writeIORef (refModified h) False

    h' = hPayload h
    write :: Node Encoding RawPointer -> StateT Int IO RawPointer
    write f = do
      ofs <- get
      let e = encodeNode f
      liftIO $ hPutEncoding h' e
      put $! ofs + getSize e
      return $ RP ofs (getSize e)

fetchKeyT :: KeyPointer -> Transaction Key
fetchKeyT p = gets dbHandle >>= \h -> liftIO (fetchKey h p)

fetchStage :: StagePointer -> Transaction (Node Encoding StagePointer)
fetchStage (Commited p) = do
  h <- gets dbHandle
  liftIO $ bimap bytes Commited <$> fetchNode h p
fetchStage (Uncommited i) = gets pending >>= return . maybe (error "fetch: not found") id . IM.lookup i

insertF :: Key
  -> (Spine StagePointer -> Transaction (Spine StagePointer))
  -> Node Encoding StagePointer
  -> Transaction (Result StagePointer)
insertF k u Empty = Pure <$> (Leaf1 <$> allocKey k <*> u [])
insertF k u (Leaf1 pk pv) = do
  vpk <- fetchKeyT pk
  Pure <$> case compare k vpk of
    LT -> do
      kp <- allocKey k
      (\v -> Leaf2 kp v pk pv) <$> u []
    EQ -> Leaf1 pk <$> u pv
    GT -> do
      kp <- allocKey k
      Leaf2 pk pv kp <$> u []
insertF k u (Leaf2 pk pv qk qv) = do
  vpk <- fetchKeyT pk
  case compare k vpk of
    LT -> do
      v <- u []
      kp <- allocKey k
      l <- addNode $ Leaf1 kp v
      r <- addNode $ Leaf1 qk qv
      return $ Carry l pk pv r
    EQ -> do
      v <- u pv
      return $ Pure $ Leaf2 pk v qk qv
    GT -> do
      vqk <- fetchKeyT qk
      case compare k vqk of
        LT -> do
          v <- u []
          l <- addNode $ Leaf1 pk pv
          r <- addNode $ Leaf1 qk qv
          kp <- allocKey k
          return $ Carry l kp v r
        EQ -> do
          v <- u qv
          return $ Pure $ Leaf2 pk pv qk v
        GT -> do
          v <- u []
          kp <- allocKey k
          l <- addNode $ Leaf1 pk pv
          r <- addNode $ Leaf1 kp v
          return $ Carry l qk qv r
insertF k u (Node2 l pk0 pv0 r) = do
  vpk0 <- fetchKeyT pk0
  case compare k vpk0 of
    LT -> do
      fl <- fetchStage l
      insertF k u fl >>= \case
        Pure l' -> do
          l'' <- addNode l'
          return $ Pure $ Node2 l'' pk0 pv0 r
        Carry l' ck cv r' -> return $ Pure $ Node3 l' ck cv r' pk0 pv0 r
    EQ -> do
      v <- u pv0
      return $ Pure $ Node2 l pk0 v r
    GT -> do
      fr <- fetchStage r
      insertF k u fr >>= \case
        Pure r' -> do
          r'' <- addNode r'
          return $ Pure $ Node2 l pk0 pv0 r''
        Carry l' ck cv r' -> return $ Pure $ Node3 l pk0 pv0 l' ck cv r'
insertF k u (Node3 l pk0 pv0 m qk0 qv0 r) = do
  vpk0 <- fetchKeyT pk0
  case compare k vpk0 of
    LT -> do
      fl <- fetchStage l
      insertF k u fl >>= \case
        Pure l' -> do
          l'' <- addNode l'
          return $ Pure $ Node3 l'' pk0 pv0 m qk0 qv0 r
        Carry l' ck cv r' -> do
          bl <- addNode (Node2 l' ck cv r')
          br <- addNode (Node2 m qk0 qv0 r)
          return $ Pure $ Node2 bl pk0 pv0 br
    EQ -> do
      v <- u pv0
      return $ Pure $ Node3 l pk0 v m qk0 qv0 r
    GT -> do
      vqk0 <- fetchKeyT qk0
      case compare k vqk0 of
        LT -> do
          fm <- fetchStage m
          insertF k u fm >>= \case
            Pure m' -> do
              m'' <- addNode m'
              return $ Pure $ Node3 l pk0 pv0 m'' qk0 qv0 r
            Carry l' ck cv r' -> do
              bl <- addNode $ Node2 l pk0 pv0 l'
              br <- addNode $ Node2 r' qk0 qv0 r
              return $ Pure $ Node2 bl ck cv br
        EQ -> do
          v <- u qv0
          return $ Pure $ Node3 l pk0 pv0 m qk0 v r
        GT -> do
          fr <- fetchStage r
          insertF k u fr >>= \case
            Pure r' -> do
              r'' <- addNode r'
              return $ Pure $ Node3 l pk0 pv0 m qk0 qv0 r''
            Carry l' ck cv r' -> do
              bl <- addNode $ Node2 l pk0 pv0 m
              br <- addNode $ Node2 l' ck cv r'
              return $ Pure $ Node2 bl qk0 qv0 br
insertF _ _ (Bin _ _ _ _) = fail "Unexpected Tree"
insertF _ _ (Tip _ _) = fail "Unexpected Leaf"

data Result a = Pure (Node Encoding a)
  | Carry !a !KeyPointer !(Spine a) !a

insertSpine :: Encoding -> RawPointer -> Spine StagePointer -> Transaction (Spine StagePointer)
insertSpine tag p ((m, x) : (n, y) : ss) | m == n = do
  t <- addNode $ Bin tag p x y
  return $ (2 * m + 1, t) : ss
insertSpine tag p ss = do
  t <- addNode $ Tip tag p
  return $ (1, t) : ss

--------------------------------------------------------------------------------
-- Fetching

class Fetchable a where
  fetchNode :: a -> RawPointer -> IO (Node Tag RawPointer)
  fetchKey :: a -> KeyPointer -> IO Key
  fetchRoot :: a -> IO (Node Tag RawPointer)
  fetchPayload :: a -> RawPointer -> IO B.ByteString

instance Fetchable LisztHandle where
  fetchNode h (RP ofs len) = fetchNode'
    (hSeek (hPayload h) AbsoluteSeek (fromIntegral ofs)) h len

  fetchKey LisztHandle{..} (KeyPointer (RP ofs len)) = do
    cache <- readIORef keyCache
    case IM.lookup ofs cache of
      Just key -> return key
      Nothing -> do
        hSeek hPayload AbsoluteSeek (fromIntegral ofs)
        key <- B.hGet hPayload len
        modifyIORef' keyCache (IM.insert ofs key)
        return key

  fetchRoot h = fetchNode'
    (hSeek (hPayload h) SeekFromEnd (-fromIntegral footerSize)) h footerSize

  fetchPayload LisztHandle{..} (RP ofs len) = do
    hSeek hPayload AbsoluteSeek (fromIntegral ofs)
    B.hGet hPayload len

fetchNode' :: IO () -> LisztHandle -> Int -> IO (Node Tag RawPointer)
fetchNode' seek h len = modifyMVar (refBuffer h) $ \(blen, buf) -> do
  seek
  (blen', buf') <- if blen < len
    then do
      buf' <- B.mallocByteString len
      return (len, buf')
    else return (blen, buf)
  f <- withForeignPtr buf' $ \ptr -> do
    _ <- hGetBuf (hPayload h) ptr len
    peekNode ptr
  return ((blen', buf'), f)
{-# INLINE fetchNode' #-}

lookupSpine :: Fetchable a => a -> Key -> Node tag RawPointer -> IO (Maybe (Spine RawPointer))
lookupSpine h k (Leaf1 p v) = do
  vp <- fetchKey h p
  if k == vp then return (Just v) else return Nothing
lookupSpine h k (Leaf2 p u q v) = do
  vp <- fetchKey h p
  if k == vp then return (Just u) else do
    vq <- fetchKey h q
    return $ if k == vq then Just v else Nothing
lookupSpine h k (Node2 l p v r) = do
  vp <- fetchKey h p
  case compare k vp of
    LT -> fetchNode h l >>= lookupSpine h k
    EQ -> return (Just v)
    GT -> fetchNode h r >>= lookupSpine h k
lookupSpine h k (Node3 l p u m q v r) = do
  vp <- fetchKey h p
  case compare k vp of
    LT -> fetchNode h l >>= lookupSpine h k
    EQ -> return (Just u)
    GT -> do
      vq <- fetchKey h q
      case compare k vq of
        LT -> fetchNode h m >>= lookupSpine h k
        EQ -> return (Just v)
        GT -> fetchNode h r >>= lookupSpine h k
lookupSpine _ _ _ = return Nothing

traverseNode :: (MonadIO m, Fetchable a) => a -> (Key -> Spine RawPointer -> m ()) -> Node Tag RawPointer -> m ()
traverseNode h f = go where
  go (Leaf1 p v) = do
    vp <- liftIO $ fetchKey h p
    f vp v
  go (Leaf2 p u q v) = do
    vp <- liftIO $ fetchKey h p
    f vp u
    vq <- liftIO $ fetchKey h q
    f vq v
  go (Node2 l p v r) = do
    liftIO (fetchNode h l) >>= go
    vp <- liftIO $ fetchKey h p
    f vp v
    liftIO (fetchNode h r) >>= go
  go (Node3 l p u m q v r) = do
    liftIO (fetchNode h l) >>= go
    vp <- liftIO $ fetchKey h p
    f vp u
    liftIO (fetchNode h m) >>= go
    vq <- liftIO $ fetchKey h q
    f vq v
    liftIO (fetchNode h r) >>= go
  go _ = pure ()

availableKeys :: Fetchable a => a -> Node t RawPointer -> IO [Key]
availableKeys _ Empty = return []
availableKeys h (Leaf1 k _) = pure <$> fetchKey h k
availableKeys h (Leaf2 j _ k _) = sequence [fetchKey h j, fetchKey h k]
availableKeys h (Node2 l k _ r) = do
  lks <- fetchNode h l >>= availableKeys h
  vk <- fetchKey h k
  rks <- fetchNode h r >>= availableKeys h
  return $ lks ++ vk : rks
availableKeys h (Node3 l j _ m k _ r) = do
  lks <- fetchNode h l >>= availableKeys h
  vj <- fetchKey h j
  mks <- fetchNode h m >>= availableKeys h
  vk <- fetchKey h k
  rks <- fetchNode h r >>= availableKeys h
  return $ lks ++ vj : mks ++ vk : rks
availableKeys _ _ = fail "availableKeys: unexpected frame"

--------------------------------------------------------------------------------
-- Footer (root node)

footerSize :: Int
footerSize = 256

emptyFooter :: B.ByteString
emptyFooter = B.pack  [0,14,171,160,140,150,185,18,22,70,203,145,129,232,42,76,81,176,163,195,96,209,8,74,97,123,57,136,107,174,241,142,100,164,181,138,253,170,25,77,12,191,212,224,142,167,215,73,48,0,2,170,226,114,8,29,141,85,243,179,81,11,59,246,62,189,202,254,56,140,227,195,189,118,152,26,106,81,4,121,152,72,247,119,111,128,75,242,29,96,157,190,170,1,57,77,61,132,72,8,233,94,254,18,197,152,128,15,174,9,91,78,125,21,72,250,179,176,176,47,230,45,255,228,214,223,28,61,123,159,104,233,131,39,88,245,13,242,228,48,17,119,159,173,71,172,238,69,137,141,153,133,79,24,81,242,19,21,209,44,120,69,180,103,100,185,189,191,50,132,52,229,248,12,207,134,45,241,2,217,112,21,239,65,39,30,33,16,147,152,52,204,221,56,87,191,235,235,173,181,106,165,37,52,245,221,13,80,91,207,224,95,157,222,3,210,54,28,99,1,7,50,189,163,141,244,200,101,250,61,48,10,243,248,153,98,224,73,227,121,72,156,228,205,43,82,166,48,85,132,0,76,73,83,90,84]

isFooter :: B.ByteString -> Bool
isFooter bs = B.drop (footerSize - 64) bs == B.drop (footerSize - 64) emptyFooter

--------------------------------------------------------------------------------
-- Spine operations

spineLength :: Spine a -> Int
spineLength = sum . map fst

type QueryResult = (Tag, RawPointer)

dropSpine :: Fetchable a => a -> Int -> Spine RawPointer -> IO (Spine RawPointer)
dropSpine _ _ [] = return []
dropSpine _ 0 s = return s
dropSpine h n0 ((siz0, t0) : xs0)
  | siz0 <= n0 = dropSpine h (n0 - siz0) xs0
  | otherwise = dropBin n0 siz0 t0 xs0
  where
    dropBin 0 siz t xs = return $ (siz, t) : xs
    dropBin n siz t xs = fetchNode h t >>= \case
      Bin _ _ l r
        | n == 1 -> return $ (siz', l) : (siz', r) : xs
        | n <= siz' -> dropBin (n - 1) siz' l ((siz', r) : xs)
        | otherwise -> dropBin (n - siz' - 1) siz' r xs
      Tip _ _ -> return xs
      _ -> error $ "dropTree: unexpected frame"
      where
        siz' = siz `div` 2

takeSpine :: Fetchable a => a -> Int -> Spine RawPointer -> [QueryResult] -> IO [QueryResult]
takeSpine _ n _ ps | n <= 0 = return ps
takeSpine _ _ [] ps = return ps
takeSpine h n ((siz, t) : xs) ps
  | n >= siz = takeAll h t ps >>= takeSpine h (n - siz) xs
  | otherwise = takeBin h n siz t ps

takeBin :: Fetchable a => a -> Int -> Int -> RawPointer -> [QueryResult] -> IO [QueryResult]
takeBin _ n _ _ ps | n <= 0 = return ps
takeBin h n siz t ps = fetchNode h t >>= \case
  Bin tag p l r
    | n == 1 -> return $ (tag, p) : ps
    | n <= siz' -> takeBin h (n - 1) siz' l ((tag, p) : ps)
    | otherwise -> do
      ps' <- takeAll h l ((tag, p) : ps)
      takeBin h (n - siz' - 1) siz' r ps'
  Tip tag p -> return $ (tag, p) : ps
  _ -> error $ "takeTree: unexpected frame"
  where
    siz' = siz `div` 2

wholeSpine :: Fetchable a => a -> Spine RawPointer -> [QueryResult] -> IO [QueryResult]
wholeSpine h ((_, s) : ss) r = wholeSpine h ss r >>= takeAll h s
wholeSpine _ [] r = return r

takeAll :: Fetchable a => a -> RawPointer -> [QueryResult] -> IO [QueryResult]
takeAll h t ps = fetchNode h t >>= \case
  Bin tag p l r -> takeAll h l ((tag, p) : ps) >>= takeAll h r
  Tip tag p -> return ((tag, p) : ps)
  _ -> error $ "takeAll: unexpected frame"

takeSpineWhile :: Fetchable a
  => (Tag -> Bool)
  -> a
  -> Spine RawPointer
  -> [QueryResult] -> IO [QueryResult]
takeSpineWhile cond h = go where
  go (t0 : ((siz, t) : xs)) ps = fetchNode h t >>= \case
    Tip tag p
      | cond tag -> takeAll h (snd t0) ps >>= go xs . ((tag, p):)
      | otherwise -> inner t0 ps
    Bin tag p l r
      | cond tag -> takeAll h (snd t0) ps
        >>= go ((siz', l) : (siz', r) : xs) . ((tag, p):)
      | otherwise -> inner t0 ps
    _ -> error "takeSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2
  go [t] ps = inner t ps
  go [] ps = return ps

  inner (siz, t) ps = fetchNode h t >>= \case
    Tip tag p
      | cond tag -> return $ (tag, p) : ps
      | otherwise -> return ps
    Bin tag p l r
      | cond tag -> go [(siz', l), (siz', r)] ((tag, p) : ps)
      | otherwise -> return ps
    _ -> error "takeSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2

dropSpineWhile :: Fetchable a
  => (Tag -> Bool)
  -> a
  -> Spine RawPointer
  -> IO (Maybe (Int, QueryResult, Spine RawPointer))
dropSpineWhile cond h = go 0 where
  go !total (t0@(siz0, _) : ts@((siz, t) : xs)) = fetchNode h t >>= \case
    Tip tag _
      | cond tag -> go (total + siz0 + siz) xs
      | otherwise -> dropBin total t0 ts
    Bin tag _ l r
      | cond tag -> go (total + siz0 + 1) $ (siz', l) : (siz', r) : xs
      | otherwise -> dropBin total t0 ts
    _ -> error "dropSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2
  go total (t : ts) = dropBin total t ts
  go _ [] = return Nothing

  dropBin !total (siz, t) ts = fetchNode h t >>= \case
    Tip tag p
      | cond tag -> go (total + 1) ts
      | otherwise -> return $ Just (total, (tag, p), ts)
    Bin tag p l r
      | cond tag -> go (total + 1) $ (siz', l) : (siz', r) : ts
      | otherwise -> return $ Just (total, (tag, p), (siz', l) : (siz', r) : ts)
    _ -> error "dropSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2
