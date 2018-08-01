{-# LANGUAGE LambdaCase, DeriveFunctor, DeriveGeneric #-}
module Database.Liszt.Internal (
  Key
  , TransactionState
  , Transaction
  , insert
  , commit
  , Frame
  , Spine
  , decodeFrame
  , lookupSpine
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

type Spine a = [(Int, a)]

instance Show Encoding where
  show e = "<Encoding " ++ show (WB.getSize e) ++ ">"

data Frame a = Empty
  | Payload !Encoding
  | Leaf1 !Key !(Spine a)
  | Leaf2 !Key !(Spine a) !Key !(Spine a)
  | Node2 !a !Key !(Spine a) !a
  | Node3 !a !Key !(Spine a) !a !Key !(Spine a) !a
  | Tree !a !a !a
  deriving (Generic, Functor, Show)

insert :: Key -> Encoding -> Transaction ()
insert key payload = do
  ofs <- addFrame (Payload payload)
  root <- gets currentRoot
  insertF key (insertSpine ofs) root >>= \case
    Pure t -> modify $ \ts -> ts { currentRoot = t }
    Carry l k a r -> modify $ \ts -> ts { currentRoot = Node2 l k a r }

type RawPointer = (Int, Int)

fetchRoot :: Handle -> IO (Frame RawPointer)
fetchRoot h = do
  hSeek h SeekFromEnd 2048
  bs <- B.hGetSome h 2048
  return $! if B.null bs
    then Empty
    else decodeFrame bs

commit :: Handle -> Transaction () -> IO ()
commit h transaction = do
  root <- fmap Commited <$> fetchRoot h
  TS _ _ pendings root' <- execStateT transaction $ TS h 0 Map.empty root

  let substP (Commited ofs) = return ofs
      substP (Uncommited i) = case Map.lookup i pendings of
        Just f -> substF f
        Nothing -> error "panic!"
      substF Empty = return (0, 0)
      substF (Payload p) = write (Payload p)
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
      substF (Tree p l r) = do
        p' <- substP p
        l' <- substP l
        r' <- substP r
        write (Tree p' l' r')

  offset0 <- fromIntegral <$> hTell h
  (_, len) <- evalStateT (substF root') offset0
  B.hPutStr h $ B.replicate (2048 - len) 0
  hFlush h
  where
    write :: Frame RawPointer -> StateT Int IO RawPointer
    write f = do
      ofs <- get
      let e = toEncoding f
      liftIO $ WB.hPut h e
      put $! ofs + WB.getSize e
      return (ofs, WB.getSize e)

lookupSpine :: Handle -> Key -> Frame RawPointer -> IO (Maybe (Spine RawPointer))
lookupSpine _ k (Leaf1 p v) | k == p = return (Just v)
lookupSpine _ k (Leaf2 p u q v)
  | k == p = return (Just u)
  | k == q = return (Just v)
lookupSpine h k (Node2 l p v r) = case compare k p of
  LT -> fetchRaw h l >>= lookupSpine h k
  EQ -> return (Just v)
  GT -> fetchRaw h r >>= lookupSpine h k
lookupSpine h k (Node3 l p u m q v r) = case compare k p of
  LT -> fetchRaw h l >>= lookupSpine h k
  EQ -> return (Just u)
  GT -> case compare k q of
    LT -> fetchRaw h m >>= lookupSpine h k
    EQ -> return (Just v)
    GT -> fetchRaw h r >>= lookupSpine h k
lookupSpine _ _ _ = return Nothing

instance Serialise a => Serialise (Frame a)

decodeFrame :: B.ByteString -> Frame RawPointer
decodeFrame = either (error . show) id $ getDecoder
  $ schema (Proxy :: Proxy (Frame RawPointer))

data Pointer = Commited !RawPointer | Uncommited !Int

data TransactionState = TS
  { dbHandle :: Handle
  , freshId :: Int
  , pending :: Map.Map Int (Frame Pointer)
  , currentRoot :: Frame Pointer
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
fetch (Uncommited i) = gets pending >>= return . maybe (error "not found") id . Map.lookup i

data Result a = Pure (Frame a)
  | Carry !a !Key !(Spine a) !a

insertSpine :: Pointer -> Spine Pointer -> Transaction (Spine Pointer)
insertSpine p ((m, x) : (n, y) : ss) | m == n = do
  t <- addFrame $ Tree p x y
  return $ (2 * m + 1, t) : ss
insertSpine p ss = return $ (1, p) : ss

insertF :: Key -> (Spine Pointer -> Transaction (Spine Pointer)) -> Frame Pointer -> Transaction (Result Pointer)
insertF k u Empty = Pure <$> Leaf1 k <$> u []
insertF k u (Leaf1 pk pv) = Pure <$> case compare k pk of
  LT -> (\v -> Leaf2 k v pk pv) <$> u []
  EQ -> Leaf1 k <$> u pv
  GT -> Leaf2 pk pv k <$> u []
insertF k u (Leaf2 pk pv qk qv) = case compare k pk of
  LT -> do
    v <- u []
    l <- addFrame $ Leaf1 k v
    r <- addFrame $ Leaf1 qk qv
    return $ Carry l pk pv r
  EQ -> do
    v <- u pv
    return $ Pure $ Leaf2 k v qk qv
  GT -> case compare k qk of
    LT -> do
      v <- u []
      l <- addFrame $ Leaf1 pk pv
      r <- addFrame $ Leaf1 qk qv
      return $ Carry l k v r
    EQ -> do
      v <- u qv
      return $ Pure $ Leaf2 pk pv k v
    GT -> do
      v <- u []
      l <- addFrame $ Leaf1 pk pv
      r <- addFrame $ Leaf1 k v
      return $ Carry l qk qv r
insertF k u (Node2 l pk0 pv0 r) = case compare k pk0 of
  LT -> do
    fl <- fetch l
    insertF k u fl >>= \case
      Pure l' -> do
        l'' <- addFrame l'
        return $ Pure $ Node2 l'' pk0 pv0 r
      Carry l' ck cv r' -> return $ Pure $ Node3 l' ck cv r' pk0 pv0 r
  EQ -> do
    v <- u pv0
    return $ Pure $ Node2 l k v r
  GT -> do
    fr <- fetch r
    insertF k u fr >>= \case
      Pure r' -> do
        r'' <- addFrame r'
        return $ Pure $ Node2 l pk0 pv0 r''
      Carry l' ck cv r' -> return $ Pure $ Node3 l pk0 pv0 l' ck cv r'
insertF k u (Node3 l pk0 pv0 m qk0 qv0 r) = case compare k pk0 of
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
    return $ Pure $ Node3 l k v m qk0 qv0 r
  GT -> case compare k qk0 of
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
      return $ Pure $ Node3 l pk0 pv0 m k v r
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
insertF _ _ (Payload _) = fail "Impossible"
insertF _ _ (Tree _ _ _) = fail "Impossible"

testRead :: FilePath -> Key -> Int -> Int -> IO ()
testRead path k i j = withBinaryFile path ReadMode $ \h -> do
  root <- fetchRoot h
  dumpFrame root
  queryRange h root k i j

queryRange :: Handle -> Frame RawPointer -> Key -> Int -> Int -> IO ()
queryRange h root key offset wanted = do
  spine <- lookupSpine h key root >>= \case
    Nothing -> fail "Not found"
    Just s -> return s

  let dropSpine :: Int -> Spine RawPointer -> IO ()
      dropSpine n ((siz, t) : xs)
        | siz <= n = dropSpine (n - siz) xs
        | otherwise = dropTree n siz t xs

      dropTree :: Int -> Int -> RawPointer -> Spine RawPointer -> IO ()
      dropTree 0 siz t xs = takeSpine wanted ((siz, t) : xs)
      dropTree n siz t xs = fetchRaw h t >>= \case
        Tree p l r
          | n == 1 -> takeSpine wanted ((siz', l) : (siz', r) : xs)
          | n < siz' -> dropTree n siz' l ((siz', r) : xs)
          | otherwise -> dropTree (n - siz' - 1) siz' r xs
        where
          siz' = pred siz `div` 2

      takeSpine :: Int -> Spine RawPointer -> IO ()
      takeSpine _ [] = return ()
      takeSpine n ((siz, t) : xs)
        | n >= siz = takeAll t >> takeSpine (n - siz) xs
        | otherwise = takeTree n siz t

      takeTree n siz t = fetchRaw h t >>= \case
        Tree p l r
          | n == 1 -> fetchRaw h p >>= dumpFrame
          | n < siz' -> takeTree n siz' l
          | otherwise -> do
            fetchRaw h p >>= dumpFrame
            takeAll l
            takeTree (n - siz' - 1) siz' r
        where
          siz' = pred siz `div` 2
      takeAll t = fetchRaw h t >>= \case
        Tree p l r -> do
          fetchRaw h p >>= dumpFrame
          takeAll l
          takeAll r
        Payload e -> print $ WB.toByteString e

  dropSpine offset spine

dumpFrame = print
