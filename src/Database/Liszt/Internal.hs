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
  -- * Frame
  , Frame(..)
  , decodeFrame
  , forceSpine
  -- * Fetching
  , Fetchable(..)
  , RawPointer(..)
  , fetchPayload
  -- * Footer
  , fetchRoot
  , footerSize
  , isFooter
  , lookupSpine
  -- * Spine
  , Spine
  , spineLength
  , QueryResult
  , takeAll
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
import Data.Proxy
import Data.Word
import Data.Winery
import qualified Data.Winery.Internal.Builder as WB
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

newtype KeyPointer = KeyPointer RawPointer deriving (Show, Eq, Serialise, NFData)

data RawPointer = RP !Int !Int deriving (Show, Eq, Generic)
instance Serialise RawPointer
instance NFData RawPointer where
  rnf r = r `seq` ()

data Frame t a = Empty
  | Leaf1 !KeyPointer !(Spine a)
  | Leaf2 !KeyPointer !(Spine a) !KeyPointer !(Spine a)
  | Node2 !a !KeyPointer !(Spine a) !a
  | Node3 !a !KeyPointer !(Spine a) !a !KeyPointer !(Spine a) !a
  | Tree !t !RawPointer !a !a
  | Leaf !t !RawPointer
  deriving (Generic, Show, Eq, Functor, Foldable, Traversable)

instance Bifunctor Frame where
  bimap f g = \case
    Empty -> Empty
    Leaf1 k s -> Leaf1 k (map (fmap g) s)
    Leaf2 j s k t -> Leaf2 j (map (fmap g) s) k (map (fmap g) t)
    Node2 l k s r -> Node2 (g l) k (map (fmap g) s) (g r)
    Node3 l j s m k t r -> Node3 (g l) j (map (fmap g) s) (g m) k (map (fmap g) t) (g r)
    Tree t p l r -> Tree (f t) p (g l) (g r)
    Leaf t p -> Leaf (f t) p

instance (Serialise t, Serialise a) => Serialise (Frame t a)

decodeFrame :: B.ByteString -> Frame Tag RawPointer
decodeFrame = either (error . show) id $ getDecoder
  $ schema (Proxy :: Proxy (Frame Tag RawPointer))

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
  , pending :: !(IM.IntMap (Frame Encoding StagePointer))
  , currentRoot :: !(Frame Encoding StagePointer)
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
  modify $ \ts -> ts { currentPos = ofs + WB.getSize payload }
  insertF key (insertSpine tag (ofs `RP` WB.getSize payload)) root >>= \case
    Pure t -> modify $ \ts -> ts { currentRoot = t }
    Carry l k a r -> modify $ \ts -> ts { currentRoot = Node2 l k a r }

addFrame :: Frame Encoding StagePointer -> Transaction StagePointer
addFrame f = state $ \ts -> (Uncommited (freshId ts), ts
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
      $ TS h 0 IM.empty (bimap WB.bytes Commited root) offset0

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
        substF (Tree t p l r) = do
          l' <- substP l
          r' <- substP r
          write (Tree t p l' r')
        substF (Leaf t p) = write (Leaf t p)

    writeFooter offset1 $ substF root'
    return ((), a)
    `onException` writeFooter offset0 (write $ first WB.bytes root)
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
    write :: Frame Encoding RawPointer -> StateT Int IO RawPointer
    write f = do
      ofs <- get
      let e = toEncoding f
      liftIO $ hPutEncoding h' e
      put $! ofs + WB.getSize e
      return $ RP ofs (WB.getSize e)

fetchKeyT :: KeyPointer -> Transaction Key
fetchKeyT p = gets dbHandle >>= \h -> liftIO (fetchKey h p)

fetchStage :: StagePointer -> Transaction (Frame Encoding StagePointer)
fetchStage (Commited p) = do
  h <- gets dbHandle
  liftIO $ bimap WB.bytes Commited <$> fetchFrame h p
fetchStage (Uncommited i) = gets pending >>= return . maybe (error "fetch: not found") id . IM.lookup i

insertF :: Key
  -> (Spine StagePointer -> Transaction (Spine StagePointer))
  -> Frame Encoding StagePointer
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
      l <- addFrame $ Leaf1 kp v
      r <- addFrame $ Leaf1 qk qv
      return $ Carry l pk pv r
    EQ -> do
      v <- u pv
      return $ Pure $ Leaf2 pk v qk qv
    GT -> do
      vqk <- fetchKeyT qk
      case compare k vqk of
        LT -> do
          v <- u []
          l <- addFrame $ Leaf1 pk pv
          r <- addFrame $ Leaf1 qk qv
          kp <- allocKey k
          return $ Carry l kp v r
        EQ -> do
          v <- u qv
          return $ Pure $ Leaf2 pk pv qk v
        GT -> do
          v <- u []
          kp <- allocKey k
          l <- addFrame $ Leaf1 pk pv
          r <- addFrame $ Leaf1 kp v
          return $ Carry l qk qv r
insertF k u (Node2 l pk0 pv0 r) = do
  vpk0 <- fetchKeyT pk0
  case compare k vpk0 of
    LT -> do
      fl <- fetchStage l
      insertF k u fl >>= \case
        Pure l' -> do
          l'' <- addFrame l'
          return $ Pure $ Node2 l'' pk0 pv0 r
        Carry l' ck cv r' -> return $ Pure $ Node3 l' ck cv r' pk0 pv0 r
    EQ -> do
      v <- u pv0
      return $ Pure $ Node2 l pk0 v r
    GT -> do
      fr <- fetchStage r
      insertF k u fr >>= \case
        Pure r' -> do
          r'' <- addFrame r'
          return $ Pure $ Node2 l pk0 pv0 r''
        Carry l' ck cv r' -> return $ Pure $ Node3 l pk0 pv0 l' ck cv r'
insertF k u (Node3 l pk0 pv0 m qk0 qv0 r) = do
  vpk0 <- fetchKeyT pk0
  case compare k vpk0 of
    LT -> do
      fl <- fetchStage l
      insertF k u fl >>= \case
        Pure l' -> do
          l'' <- addFrame l'
          return $ Pure $ Node3 l'' pk0 pv0 m qk0 qv0 r
        Carry l' ck cv r' -> do
          bl <- addFrame (Node2 l' ck cv r')
          br <- addFrame (Node2 m qk0 qv0 r)
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
              m'' <- addFrame m'
              return $ Pure $ Node3 l pk0 pv0 m'' qk0 qv0 r
            Carry l' ck cv r' -> do
              bl <- addFrame $ Node2 l pk0 pv0 l'
              br <- addFrame $ Node2 r' qk0 qv0 r
              return $ Pure $ Node2 bl ck cv br
        EQ -> do
          v <- u qv0
          return $ Pure $ Node3 l pk0 pv0 m qk0 v r
        GT -> do
          fr <- fetchStage r
          insertF k u fr >>= \case
            Pure r' -> do
              r'' <- addFrame r'
              return $ Pure $ Node3 l pk0 pv0 m qk0 qv0 r''
            Carry l' ck cv r' -> do
              bl <- addFrame $ Node2 l pk0 pv0 m
              br <- addFrame $ Node2 l' ck cv r'
              return $ Pure $ Node2 bl qk0 qv0 br
insertF _ _ (Tree _ _ _ _) = fail "Unexpected Tree"
insertF _ _ (Leaf _ _) = fail "Unexpected Leaf"

data Result a = Pure (Frame Encoding a)
  | Carry !a !KeyPointer !(Spine a) !a

insertSpine :: Encoding -> RawPointer -> Spine StagePointer -> Transaction (Spine StagePointer)
insertSpine tag p ((m, x) : (n, y) : ss) | m == n = do
  t <- addFrame $ Tree tag p x y
  return $ (2 * m + 1, t) : ss
insertSpine tag p ss = do
  t <- addFrame $ Leaf tag p
  return $ (1, t) : ss

--------------------------------------------------------------------------------
-- Fetching

class Fetchable a where
  fetchFrame :: LisztHandle -> a -> IO (Frame Tag a)

instance Fetchable RawPointer where
  fetchFrame h (RP ofs len) = fetchFrame'
    (hSeek (hPayload h) AbsoluteSeek (fromIntegral ofs)) h len

fetchKey :: LisztHandle -> KeyPointer -> IO Key
fetchKey LisztHandle{..} (KeyPointer (RP ofs len)) = do
  cache <- readIORef keyCache
  case IM.lookup ofs cache of
    Just key -> return key
    Nothing -> do
      hSeek hPayload AbsoluteSeek (fromIntegral ofs)
      key <- B.hGet hPayload len
      modifyIORef' keyCache (IM.insert ofs key)
      return key

fetchFrame' :: IO () -> LisztHandle -> Int -> IO (Frame Tag RawPointer)
fetchFrame' seek h len = modifyMVar (refBuffer h) $ \(blen, buf) -> do
  seek
  (blen', buf') <- if blen < len
    then do
      buf' <- B.mallocByteString len
      return (len, buf')
    else return (blen, buf)
  f <- withForeignPtr buf' $ \ptr -> do
    _ <- hGetBuf (hPayload h) ptr len
    let f = decodeFrame $ B.PS buf' 0 len
    forceSpine f
  return ((blen', buf'), f)
{-# INLINE fetchFrame' #-}

lookupSpine :: Fetchable a => LisztHandle -> Key -> Frame tag a -> IO (Maybe (Spine a))
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
    LT -> fetchFrame h l >>= lookupSpine h k
    EQ -> return (Just v)
    GT -> fetchFrame h r >>= lookupSpine h k
lookupSpine h k (Node3 l p u m q v r) = do
  vp <- fetchKey h p
  case compare k vp of
    LT -> fetchFrame h l >>= lookupSpine h k
    EQ -> return (Just u)
    GT -> do
      vq <- fetchKey h q
      case compare k vq of
        LT -> fetchFrame h m >>= lookupSpine h k
        EQ -> return (Just v)
        GT -> fetchFrame h r >>= lookupSpine h k
lookupSpine _ _ _ = return Nothing

availableKeys :: LisztHandle -> Frame t RawPointer -> IO [Key]
availableKeys _ Empty = return []
availableKeys h (Leaf1 k _) = pure <$> fetchKey h k
availableKeys h (Leaf2 j _ k _) = sequence [fetchKey h j, fetchKey h k]
availableKeys h (Node2 l k _ r) = do
  lks <- fetchFrame h l >>= availableKeys h
  vk <- fetchKey h k
  rks <- fetchFrame h r >>= availableKeys h
  return $ lks ++ vk : rks
availableKeys h (Node3 l j _ m k _ r) = do
  lks <- fetchFrame h l >>= availableKeys h
  vj <- fetchKey h j
  mks <- fetchFrame h m >>= availableKeys h
  vk <- fetchKey h k
  rks <- fetchFrame h r >>= availableKeys h
  return $ lks ++ vj : mks ++ vk : rks
availableKeys _ _ = fail "availableKeys: unexpected frame"

fetchPayload :: LisztHandle -> RawPointer -> IO B.ByteString
fetchPayload LisztHandle{..} (RP ofs len) = do
  hSeek hPayload AbsoluteSeek (fromIntegral ofs)
  B.hGet hPayload len

--------------------------------------------------------------------------------
-- Footer (root node)

footerSize :: Int
footerSize = 256

emptyFooter :: B.ByteString
emptyFooter = B.pack  [0,14,171,160,140,150,185,18,22,70,203,145,129,232,42,76,81,176,163,195,96,209,8,74,97,123,57,136,107,174,241,142,100,164,181,138,253,170,25,77,12,191,212,224,142,167,215,73,48,0,2,170,226,114,8,29,141,85,243,179,81,11,59,246,62,189,202,254,56,140,227,195,189,118,152,26,106,81,4,121,152,72,247,119,111,128,75,242,29,96,157,190,170,1,57,77,61,132,72,8,233,94,254,18,197,152,128,15,174,9,91,78,125,21,72,250,179,176,176,47,230,45,255,228,214,223,28,61,123,159,104,233,131,39,88,245,13,242,228,48,17,119,159,173,71,172,238,69,137,141,153,133,79,24,81,242,19,21,209,44,120,69,180,103,100,185,189,191,50,132,52,229,248,12,207,134,45,241,2,217,112,21,239,65,39,30,33,16,147,152,52,204,221,56,87,191,235,235,173,181,106,165,37,52,245,221,13,80,91,207,224,95,157,222,3,210,54,28,99,1,7,50,189,163,141,244,200,101,250,61,48,10,243,248,153,98,224,73,227,121,72,156,228,205,43,82,166,48,85,132,0,76,73,83,90,84]

isFooter :: B.ByteString -> Bool
isFooter bs = B.drop 128 bs == B.drop 128 emptyFooter

fetchRoot :: LisztHandle -> IO (Frame Tag RawPointer)
fetchRoot h = fetchFrame'
  (hSeek (hPayload h) SeekFromEnd (-fromIntegral footerSize)) h footerSize

--------------------------------------------------------------------------------
-- Spine operations

spineLength :: Spine a -> Int
spineLength = sum . map fst

type QueryResult = (Tag, RawPointer)

dropSpine :: Fetchable a => LisztHandle -> Int -> Spine a -> IO (Spine a)
dropSpine _ _ [] = return []
dropSpine _ 0 s = return s
dropSpine h n0 ((siz0, t0) : xs0)
  | siz0 <= n0 = dropSpine h (n0 - siz0) xs0
  | otherwise = dropTree n0 siz0 t0 xs0
  where
    dropTree 0 siz t xs = return $ (siz, t) : xs
    dropTree n siz t xs = fetchFrame h t >>= \case
      Tree _ _ l r
        | n == 1 -> return $ (siz', l) : (siz', r) : xs
        | n <= siz' -> dropTree (n - 1) siz' l ((siz', r) : xs)
        | otherwise -> dropTree (n - siz' - 1) siz' r xs
      Leaf _ _ -> return xs
      _ -> error $ "dropTree: unexpected frame"
      where
        siz' = siz `div` 2

takeSpine :: Fetchable a => LisztHandle -> Int -> Spine a -> [QueryResult] -> IO [QueryResult]
takeSpine _ n _ ps | n <= 0 = return ps
takeSpine _ _ [] ps = return ps
takeSpine h n ((siz, t) : xs) ps
  | n >= siz = takeAll h t ps >>= takeSpine h (n - siz) xs
  | otherwise = takeTree h n siz t ps

takeTree :: Fetchable a => LisztHandle -> Int -> Int -> a -> [QueryResult] -> IO [QueryResult]
takeTree _ n _ _ ps | n <= 0 = return ps
takeTree h n siz t ps = fetchFrame h t >>= \case
  Tree tag p l r
    | n == 1 -> return $ (tag, p) : ps
    | n <= siz' -> takeTree h (n - 1) siz' l ((tag, p) : ps)
    | otherwise -> do
      ps' <- takeAll h l ((tag, p) : ps)
      takeTree h (n - siz' - 1) siz' r ps'
  Leaf tag p -> return $ (tag, p) : ps
  _ -> error $ "takeTree: unexpected frame"
  where
    siz' = siz `div` 2

takeAll :: Fetchable a => LisztHandle -> a -> [QueryResult] -> IO [QueryResult]
takeAll h t ps = fetchFrame h t >>= \case
  Tree tag p l r -> takeAll h l ((tag, p) : ps) >>= takeAll h r
  Leaf tag p -> return ((tag, p) : ps)
  _ -> error $ "takeAll: unexpected frame"

takeSpineWhile :: Fetchable a
  => (Tag -> Bool)
  -> LisztHandle
  -> Spine a
  -> [QueryResult] -> IO [QueryResult]
takeSpineWhile cond h = go where
  go (t0 : ((siz, t) : xs)) ps = fetchFrame h t >>= \case
    Leaf tag p
      | cond tag -> takeAll h (snd t0) ps >>= go xs . ((tag, p):)
      | otherwise -> inner t0 ps
    Tree tag p l r
      | cond tag -> takeAll h (snd t0) ps
        >>= go ((siz', l) : (siz', r) : xs) . ((tag, p):)
      | otherwise -> inner t0 ps
    _ -> error "takeSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2
  go [t] ps = inner t ps
  go [] ps = return ps

  inner (siz, t) ps = fetchFrame h t >>= \case
    Leaf tag p
      | cond tag -> return $ (tag, p) : ps
      | otherwise -> return ps
    Tree tag p l r
      | cond tag -> go [(siz', l), (siz', r)] ((tag, p) : ps)
      | otherwise -> return ps
    _ -> error "takeSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2

dropSpineWhile :: Fetchable a
  => (Tag -> Bool)
  -> LisztHandle
  -> Spine a
  -> IO (Maybe (Int, QueryResult, Spine a))
dropSpineWhile cond h = go 0 where
  go !total (t0@(siz0, _) : ts@((siz, t) : xs)) = fetchFrame h t >>= \case
    Leaf tag _
      | cond tag -> go (total + siz0 + siz) xs
      | otherwise -> dropTree total t0 ts
    Tree tag _ l r
      | cond tag -> go (total + siz0 + 1) $ (siz', l) : (siz', r) : xs
      | otherwise -> dropTree total t0 ts
    _ -> error "dropSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2
  go total (t : ts) = dropTree total t ts
  go _ [] = return Nothing

  dropTree !total (siz, t) ts = fetchFrame h t >>= \case
    Leaf tag p
      | cond tag -> go (total + 1) ts
      | otherwise -> return $ Just (total, (tag, p), ts)
    Tree tag p l r
      | cond tag -> go (total + 1) $ (siz', l) : (siz', r) : ts
      | otherwise -> return $ Just (total, (tag, p), (siz', l) : (siz', r) : ts)
    _ -> error "dropSpineWhile: unexpected frame"
    where
      siz' = siz `div` 2

--------------------------------------------------------------------------------
-- Magical

copyByteString :: B.ByteString -> IO B.ByteString
copyByteString (B.PS fp ofs len) = do
  fp' <- B.mallocByteString len
  withForeignPtr fp' $ \dst -> withForeignPtr fp
    $ \src -> B.memcpy dst (src `plusPtr` ofs) len
  return (B.PS fp' 0 len)

forceSpine :: NFData a => Frame Tag a -> IO (Frame Tag a)
forceSpine f = case f of
  Empty -> return Empty
  Leaf1 _ s -> rnf s `seq` return f
  Leaf2 _ s _ t -> rnf s `seq` rnf t `seq` return f
  Node2 _ _ s _ -> rnf s  `seq` return f
  Node3 _ _ s _ _ t _ -> rnf s `seq` rnf t `seq` return f
  Tree tag p l r -> do
    tag' <- copyByteString tag
    return (Tree tag' p l r)
  Leaf tag p -> do
    tag' <- copyByteString tag
    return (Leaf tag' p)
