{-# LANGUAGE LambdaCase, DeriveFunctor, DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Database.Liszt.Internal (
  Key
  , Tag
  , TransactionState
  , Transaction
  , Frame
  , RawPointer
  , footerSize
  , decodeFrame
  -- * Writing
  , clear
  , insert
  , commit
  -- * Reading
  , availableKeys
  , lookupSpine
  , takeSpine
  , dropSpine
  , takeSpineWhile
  , dropSpineWhile
  ) where

import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import qualified Data.ByteString as B
import qualified Data.Map.Strict as Map
import Data.Proxy
import Data.Winery
import qualified Data.Winery.Internal.Builder as WB
import GHC.Generics (Generic)
import System.IO

type Key = B.ByteString

-- | Tag is an extra value attached to a payload. This can be used to perform
-- a binary search.
type Tag = B.ByteString

type Spine a = [(Int, a)]

newtype KeyPointer = KeyPointer RawPointer deriving Serialise

type RawPointer = (Int, Int)

data Frame a = Empty
  | Leaf1 !KeyPointer !(Spine a)
  | Leaf2 !KeyPointer !(Spine a) !KeyPointer !(Spine a)
  | Node2 !a !KeyPointer !(Spine a) !a
  | Node3 !a !KeyPointer !(Spine a) !a !KeyPointer !(Spine a) !a
  | Tree !Tag !RawPointer !a !a
  | Leaf !Tag !RawPointer
  deriving (Generic, Functor)

instance Serialise a => Serialise (Frame a)

clear :: Key -> Transaction ()
clear key = do
  root <- gets currentRoot
  insertF key (const $ return []) root >>= \case
    Pure t -> modify $ \ts -> ts { currentRoot = t }
    Carry l k a r -> modify $ \ts -> ts { currentRoot = Node2 l k a r }

insert :: Key -> B.ByteString -> Encoding -> Transaction ()
insert key tag payload = do
  h <- gets dbHandle
  liftIO $ hSeek h SeekFromEnd 0
  ofs <- liftIO $ fromIntegral <$> hTell h
  liftIO $ WB.hPut h payload
  root <- gets currentRoot
  insertF key (insertSpine tag (ofs, WB.getSize payload)) root >>= \case
    Pure t -> modify $ \ts -> ts { currentRoot = t }
    Carry l k a r -> modify $ \ts -> ts { currentRoot = Node2 l k a r }

footerSize :: Int
footerSize = 256

fetchRoot :: Handle -> IO (Frame RawPointer)
fetchRoot h = do
  size <- hFileSize h
  if size == 0
    then return Empty
    else do
      hSeek h SeekFromEnd (-fromIntegral footerSize)
      bs <- B.hGetSome h footerSize
      return $ if B.length bs == 0
        then Empty
        else if B.length bs == footerSize
          then decodeFrame bs
          else error "fetchRoot: malformed footer"

allocKey :: Key -> Transaction KeyPointer
allocKey key = do
  h <- gets dbHandle
  ofs <- liftIO $ do
    hSeek h SeekFromEnd 0
    ofs <- fromIntegral <$> hTell h
    B.hPutStr h key
    return ofs
  return $ KeyPointer (ofs, B.length key)

fetchKey :: Handle -> KeyPointer -> IO Key
fetchKey h (KeyPointer (ofs, len)) = do
  hSeek h AbsoluteSeek (fromIntegral ofs)
  B.hGet h len

fetchKeyT :: KeyPointer -> Transaction Key
fetchKeyT p = gets dbHandle >>= \h -> liftIO (fetchKey h p)

commit :: Handle -> Transaction a -> IO a
commit h transaction = do
  root <- fmap Commited <$> fetchRoot h
  (a, TS _ _ pendings root') <- runStateT transaction $ TS h 0 Map.empty root

  let substP (Commited ofs) = return ofs
      substP (Uncommited i) = case Map.lookup i pendings of
        Just f -> substF f
        Nothing -> error "panic!"
      substF Empty = return (0, 0)
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

  hSeek h SeekFromEnd 0
  offset0 <- fromIntegral <$> hTell h
  (_, len) <- evalStateT (substF root') offset0
  B.hPutStr h $ B.replicate (footerSize - len) 0
  hFlush h
  return a
  where
    write :: Frame RawPointer -> StateT Int IO RawPointer
    write f = do
      ofs <- get
      let e = toEncoding f
      liftIO $ hSeek h SeekFromEnd 0
      liftIO $ WB.hPut h e
      put $! ofs + WB.getSize e
      return (ofs, WB.getSize e)

lookupSpine :: Handle -> Key -> Frame RawPointer -> IO (Maybe (Spine RawPointer))
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
    LT -> fetchRaw h l >>= lookupSpine h k
    EQ -> return (Just v)
    GT -> fetchRaw h r >>= lookupSpine h k
lookupSpine h k (Node3 l p u m q v r) = do
  vp <- fetchKey h p
  case compare k vp of
    LT -> fetchRaw h l >>= lookupSpine h k
    EQ -> return (Just u)
    GT -> do
      vq <- fetchKey h q
      case compare k vq of
        LT -> fetchRaw h m >>= lookupSpine h k
        EQ -> return (Just v)
        GT -> fetchRaw h r >>= lookupSpine h k
lookupSpine _ _ _ = return Nothing

decodeFrame :: B.ByteString -> Frame RawPointer
decodeFrame = either (error . show) id $ getDecoder
  $ schema (Proxy :: Proxy (Frame RawPointer))

data Pointer = Commited !RawPointer | Uncommited !Int

data TransactionState = TS
  { dbHandle :: !Handle
  , freshId :: !Int
  , pending :: !(Map.Map Int (Frame Pointer))
  , currentRoot :: !(Frame Pointer)
  }

type Transaction = StateT TransactionState IO

addFrame :: Frame Pointer -> Transaction Pointer
addFrame f = state $ \ts -> (Uncommited (freshId ts), ts
  { freshId = freshId ts + 1
  , pending = Map.insert (freshId ts) f (pending ts) })

fetchRaw :: Handle -> RawPointer -> IO (Frame RawPointer)
fetchRaw h (ofs, len) = do
  hSeek h AbsoluteSeek (fromIntegral ofs)
  decodeFrame <$> B.hGet h len

fetch :: Pointer -> Transaction (Frame Pointer)
fetch (Commited p) = do
  h <- gets dbHandle
  liftIO $ fmap Commited <$> fetchRaw h p
fetch (Uncommited i) = gets pending >>= return . maybe (error "fetch: not found") id . Map.lookup i

data Result a = Pure (Frame a)
  | Carry !a !KeyPointer !(Spine a) !a

insertSpine :: B.ByteString -> RawPointer -> Spine Pointer -> Transaction (Spine Pointer)
insertSpine tag p ((m, x) : (n, y) : ss) | m == n = do
  t <- addFrame $ Tree tag p x y
  return $ (2 * m + 1, t) : ss
insertSpine tag p ss = do
  t <- addFrame $ Leaf tag p
  return $ (1, t) : ss

insertF :: Key -> (Spine Pointer -> Transaction (Spine Pointer)) -> Frame Pointer -> Transaction (Result Pointer)
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
      fl <- fetch l
      insertF k u fl >>= \case
        Pure l' -> do
          l'' <- addFrame l'
          return $ Pure $ Node2 l'' pk0 pv0 r
        Carry l' ck cv r' -> return $ Pure $ Node3 l' ck cv r' pk0 pv0 r
    EQ -> do
      v <- u pv0
      return $ Pure $ Node2 l pk0 v r
    GT -> do
      fr <- fetch r
      insertF k u fr >>= \case
        Pure r' -> do
          r'' <- addFrame r'
          return $ Pure $ Node2 l pk0 pv0 r''
        Carry l' ck cv r' -> return $ Pure $ Node3 l pk0 pv0 l' ck cv r'
insertF k u (Node3 l pk0 pv0 m qk0 qv0 r) = do
  vpk0 <- fetchKeyT pk0
  case compare k vpk0 of
    LT -> do
      fl <- fetch l
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
          fm <- fetch m
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
          fr <- fetch r
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

availableKeys :: Handle -> Frame RawPointer -> IO [Key]
availableKeys _ Empty = return []
availableKeys h (Leaf1 k _) = pure <$> fetchKey h k
availableKeys h (Leaf2 j _ k _) = sequence [fetchKey h j, fetchKey h k]
availableKeys h (Node2 l k _ r) = do
  lks <- fetchRaw h l >>= availableKeys h
  vk <- fetchKey h k
  rks <- fetchRaw h r >>= availableKeys h
  return $ lks ++ vk : rks
availableKeys h (Node3 l j _ m k _ r) = do
  lks <- fetchRaw h l >>= availableKeys h
  vj <- fetchKey h j
  mks <- fetchRaw h m >>= availableKeys h
  vk <- fetchKey h k
  rks <- fetchRaw h r >>= availableKeys h
  return $ lks ++ vj : mks ++ vk : rks
availableKeys _ _ = fail "availableKeys: unexpected frame"

type QueryResult = [(Tag, RawPointer)]

dropSpine :: Handle -> Int -> Spine RawPointer -> IO (Spine RawPointer)
dropSpine _ _ [] = return []
dropSpine h n0 ((siz0, t0) : xs0)
  | siz0 <= n0 = dropSpine h (n0 - siz0) xs0
  | otherwise = dropTree n0 siz0 t0 xs0
  where
    dropTree :: Int -> Int -> RawPointer -> Spine RawPointer -> IO (Spine RawPointer)
    dropTree 0 siz t xs = return $ (siz, t) : xs
    dropTree n siz t xs = fetchRaw h t >>= \case
      Tree _ _ l r
        | n == 1 -> return $ (siz', l) : (siz', r) : xs
        | n < siz' -> dropTree (n - 1) siz' l ((siz', r) : xs)
        | otherwise -> dropTree (n - siz' - 1) siz' r xs
      _ -> error "dropTree: unexpected frame"
      where
        siz' = siz `div` 2

takeSpine :: Handle -> Int -> Spine RawPointer -> QueryResult -> IO QueryResult
takeSpine _ n _ ps | n <= 0 = return ps
takeSpine _ _ [] ps = return ps
takeSpine h n ((siz, t) : xs) ps
  | n >= siz = takeAll h t ps >>= takeSpine h (n - siz) xs
  | otherwise = takeTree h n siz t ps

takeTree :: Handle -> Int -> Int -> RawPointer -> QueryResult -> IO QueryResult
takeTree _ n _ _ ps | n <= 0 = return ps
takeTree h n siz t ps = fetchRaw h t >>= \case
  Tree tag p l r
    | n == 1 -> return $ (tag, p) : ps
    | n <= siz' -> takeTree h (n - 1) siz' l ((tag, p) : ps)
    | otherwise -> do
      ps' <- takeAll h l ((tag, p) : ps)
      takeTree h (n - siz' - 1) siz' r ps'
  Leaf tag p -> return $ (tag, p) : ps
  _ -> error "takeTree: unexpected frame"
  where
    siz' = siz `div` 2

takeAll :: Handle -> RawPointer -> QueryResult -> IO QueryResult
takeAll h t ps = fetchRaw h t >>= \case
  Tree tag p l r -> takeAll h l ((tag, p) : ps) >>= takeAll h r
  Leaf tag p -> return ((tag, p) : ps)
  _ -> error "takeAll: unexpected frame"

takeSpineWhile :: (Tag -> Bool) -> Handle -> Spine RawPointer -> QueryResult -> IO QueryResult
takeSpineWhile cond h = go where
  go (t0 : ((siz, t) : xs)) ps = fetchRaw h t >>= \case
    Leaf tag p
      | cond tag -> takeAll h (snd t0) ps >>= go xs . ((tag, p):)
      | otherwise -> inner t0 ps
    Tree tag p l r
      | cond tag -> takeAll h (snd t0) ps
        >>= go ((siz', l) : (siz', r) : xs) . ((tag, p):)
      | otherwise -> inner t0 ps
    _ -> error "unexpected frame"
    where
      siz' = siz `div` 2
  go [t] ps = inner t ps
  go [] ps = return ps

  inner (siz, t) ps = fetchRaw h t >>= \case
    Leaf tag p
      | cond tag -> return $ (tag, p) : ps
      | otherwise -> return ps
    Tree tag p l r
      | cond tag -> go [(siz', l), (siz', r)] ((tag, p) : ps)
      | otherwise -> return ps
    _ -> error "unexpected frame"
    where
      siz' = siz `div` 2

dropSpineWhile :: (Tag -> Bool)
  -> Handle
  -> Spine RawPointer
  -> IO (Maybe ((Tag, RawPointer), Spine RawPointer))
dropSpineWhile cond h = go where
  go (t0 : ts@((siz, t) : xs)) = fetchRaw h t >>= \case
    Leaf tag _
      | cond tag -> go xs
      | otherwise -> dropTree t0 ts
    Tree tag _ l r
      | cond tag -> go $ (siz', l) : (siz', r) : xs
      | otherwise -> dropTree t0 ts
    _ -> error "unexpected frame"
    where
      siz' = siz `div` 2
  go (t : ts) = dropTree t ts
  go [] = return Nothing

  dropTree (siz, t) ts = fetchRaw h t >>= \case
    Leaf tag p
      | cond tag -> go ts
      | otherwise -> return $ Just ((tag, p), ts)
    Tree tag p l r
      | cond tag -> go $ (siz', l) : (siz', r) : ts
      | otherwise -> return $ Just ((tag, p), (siz', l) : (siz', r) : ts)
    _ -> error "unexpected frame"
    where
      siz' = siz `div` 2
