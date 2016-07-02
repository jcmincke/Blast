{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverlappingInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Blast.Syntax
(
  Joinable (..)
  , fun
  , closure
  , foldFun
  , foldClosure
  , funIO
  , closureIO
  , foldFunIO
  , foldClosureIO
  , rapply'
  , rmap
  , rflatmap
  , rfilter
  , (<**>)
  , (<$$>)
  , lfold
  , lfold'
  , rfold
  , rfold'
  , rjoin
  , count
  , KeyedVal (..)
  , rKeyedJoin
  , Range (..)
  , rangeToList

)
where

import Debug.Trace
import            Control.Monad hiding (join)
import            Control.Monad.Operational
import            Data.Foldable
import            Data.Hashable
import qualified  Data.HashMap.Lazy as M
import qualified  Data.List as L
import            Data.Maybe (catMaybes)
import            Data.Proxy
import qualified  Data.Vector as Vc
import qualified  Data.Serialize as S

import            GHC.Generics (Generic)


import            Blast.Types

class Joinable a b where
  join :: a -> b -> Maybe (a, b)


fun :: (a -> b) -> Fun e a b
fun f = Pure (return . f)

closure :: (S.Serialize c, ChunkableFreeVar c) => e 'Local c -> (c -> a -> b) -> Fun e a b
closure ce f = Closure ce (\c a -> return $ f c a)


foldFun :: (r -> a -> r) -> FoldFun e a r
foldFun f = FoldPure (\r a -> return $ f r a)

foldClosure :: (S.Serialize c, ChunkableFreeVar c) => e 'Local c -> (c -> r -> a -> r) -> FoldFun e a r
foldClosure ce f = FoldClosure ce (\c r a -> return $ f c r a)

funIO :: (a -> IO b) -> Fun k a b
funIO f = Pure f

closureIO :: (S.Serialize c, ChunkableFreeVar c) => e 'Local c -> (c -> a -> IO b) -> Fun e a b
closureIO ce f = Closure ce f


foldFunIO :: (r -> a -> IO r) -> FoldFun e a r
foldFunIO f = FoldPure f

foldClosureIO :: (S.Serialize c, ChunkableFreeVar c) => e 'Local c -> (c -> r -> a -> IO r) -> FoldFun e a r
foldClosureIO ce f = FoldClosure ce f



rapply' :: (Monad m, Builder m e) =>
        Fun e a b -> e 'Remote a -> ProgramT (Syntax m) m (e 'Remote b)
rapply' fm e  = do
  cs <- mkRemoteClosure fm
  rapply cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lconst ()
    return $ ExpClosure ue (\() a -> f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> f c a)

rmap :: (Monad m, Builder m e, Traversable t) =>
        Fun e a b -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote (t b))
rmap fm e  = do
  cs <- mkRemoteClosure fm
  rapply cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lconst ()
    return $ ExpClosure ue (\() a -> mapM f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> mapM (f c) a)

rflatmap :: (Monad m, Foldable t1, Builder m e, Monoid b) =>
            Fun e t b -> e 'Remote (t1 t) -> ProgramT (Syntax m) m (e 'Remote b)
rflatmap fp e = do
  cs <- mkRemoteClosure fp
  rapply cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lconst ()
    return $ ExpClosure ue (\() a -> foldMap f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> foldMap (f c) a)

rfilter :: (Monad m, Applicative f, Foldable t, Monoid (f a),
            Monoid (IO (f a)), Builder m e) =>
  Fun e a Bool -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote (f a))
rfilter p e = do
  cs <- mkRemoteClosure p
  rapply cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lconst ()
    return $ ExpClosure ue (\() ta -> do
        r <- foldMap (\a -> do
            b <- f a
            return $ if b then pure a else mempty) ta
        return r)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c ta -> do
        r <- foldMap (\a -> do
              b <- f c a
              return $ if b then pure a else mempty) ta
        return r)


(<**>) :: (Monad m, Builder m e)
          =>  ProgramT (Syntax m) m (e 'Local (a -> b))
          -> e 'Local a -> ProgramT (Syntax m) m (e 'Local b)
f <**> a = do
  cs <- f
  lapply cs a


(<$$>) :: (Monad m, Builder m e)
          => (a -> b) -> e 'Local a -> ProgramT (Syntax m) m (e 'Local b)
f <$$> e = lconst f <**> e


lfold ::  (Monad m, Foldable t, Builder m e)
          => e 'Local (b -> a -> b)
          -> e 'Local b -> e 'Local (t a) -> ProgramT (Syntax m) m (e 'Local b)
lfold f zero a = do
  f' <- foldl <$$> f <**> zero
  lapply f' a


lfold' :: (Monad m, Foldable t, Builder m e) =>
  (b -> a -> b)
  -> e 'Local b
  -> e 'Local (t a)
  -> ProgramT (Syntax m) m (e 'Local b)
lfold' f zero a = do
  f' <- lconst f
  lfold f' zero a

count ::  (Monad m, Foldable t, Builder m e)
          => e 'Local (t a) -> ProgramT (Syntax m) m (e 'Local Int)
count e = do
  zero <- lconst (0::Int)
  f <- lconst (\b _ -> b+1)
  lfold f zero e

rfold ::  (Builder m e, Traversable t, Applicative t, S.Serialize r, Monad m)
          => FoldFun e a r -> e 'Local r -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote ([r]))
rfold fp zero e = do
  cs <- mkRemoteClosure fp
  rapply cs e
  where
  mkRemoteClosure (FoldPure f) = do
      cv <- (\z -> ((), z)) <$$> zero
      return $ ExpClosure cv (\((), z) a -> do
                r <- foldM f z a
                return [r])
  mkRemoteClosure (FoldClosure ce f) = do
      cv <- (\c z -> (c, z)) <$$> ce <**> zero
      return $ ExpClosure cv (\(c,z) a -> do
                r <- foldM  (f c) z a
                return [r])


rfold' :: (Monad m, Applicative t, Traversable t, S.Serialize r, UnChunkable [r], Builder m e) =>
  FoldFun e a r -> ([r] -> b) -> e 'Local r -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Local b)
rfold' f aggregator zero a = do
  rs <- rfold f zero a
  ars <- collect rs
  aggregator <$$> ars



instance Joinable a b where
  join a b = Just (a, b)



instance (Eq k) => Joinable (KeyedVal k a) (KeyedVal k b) where
  join (KeyedVal k1 a) (KeyedVal k2 b) | k1 == k2 = Just (KeyedVal k1 a, KeyedVal k2 b)
  join (KeyedVal _ _) (KeyedVal _ _) = Nothing




fromList' :: (Applicative t, Foldable t, Monoid (t a)) => [a] -> t a
fromList' l = foldMap pure l

rjoin :: (Monad m, Applicative t, Foldable t, Foldable t1, Foldable t2,
          Monoid (t (a, b)), S.Serialize (t1 a), Builder m e,
          ChunkableFreeVar (t1 a), UnChunkable (t1 a), Joinable a b) =>
   e 'Remote (t1 a) -> e 'Remote (t2 b) -> ProgramT (Syntax m) m (e 'Remote (t (a, b)))
rjoin a b = do
  a' <- collect a
  let cs = ExpClosure a' (\av bv -> return $ fromList' $ catMaybes [join x y | x <- toList av, y <- toList bv])
  rapply cs b

data KeyedVal k v = KeyedVal k v
  deriving (Generic, S.Serialize, Show)


data OptiT t k v = OptiT (t (KeyedVal k v))
  deriving (Generic)

instance (Show (t (KeyedVal k v))) => Show (OptiT t k v) where
  show (OptiT x) = show x

instance (S.Serialize (t (KeyedVal k v))) => S.Serialize (OptiT t k v)

instance (Hashable k) => Chunkable [KeyedVal k v] where
  chunk nbBuckets l =
    Vc.reverse $ Vc.generate nbBuckets (\i -> buckets M.! i)
    where
    buckets = L.foldl proc M.empty l
    proc bucket' kv@(KeyedVal k _) = let
      i = hash k `mod` nbBuckets
      in M.insertWith (++) i [kv] bucket'

instance (Hashable k) => UnChunkable [KeyedVal k v] where
  unChunk l = L.concat l


instance (Applicative t, Foldable t, Monoid (t (KeyedVal k v)), Chunkable (t (KeyedVal k v))) => ChunkableFreeVar (OptiT t k v) where
  chunk' n (OptiT tkvs) = trace ("here") $ fmap OptiT $ chunk n tkvs



rKeyedJoin
  :: (Eq k, Monad m, Applicative t, Applicative t1,
      Foldable t, Foldable t1, Foldable t2,
      Monoid (t (KeyedVal k (t3, t4))), Monoid (t1 (KeyedVal k t3)),
      UnChunkable (t1 (KeyedVal k t3)), Chunkable (t1 (KeyedVal k t3)),
      Builder m e, S.Serialize (t1 (KeyedVal k t3))) =>
     Proxy t
     -> e 'Remote (t1 (KeyedVal k t3))
     -> e 'Remote (t2 (KeyedVal k t4))
     -> Control.Monad.Operational.ProgramT
          (Syntax m) m (e 'Remote (t (KeyedVal k (t3, t4))))
rKeyedJoin _ a b = do
  a' <- collect a
  ja <- OptiT <$$> a'
  let cs = ExpClosure ja (\(OptiT av) bv -> return $ fromList' $ catMaybes [doJoin x y | x <- toList av, y <- toList bv])
  rapply cs b
  where
  doJoin (KeyedVal k1 a') (KeyedVal k2 b') | k1 == k2 = Just $ KeyedVal k1 (a', b')
  doJoin (KeyedVal _ _) (KeyedVal _ _) = Nothing





data Range = Range Int Int
  deriving (Eq, Show, Generic, S.Serialize)

rangeToList :: Range -> [Int]
rangeToList (Range a b) = [a .. (b-1)]


instance Chunkable Range where
  chunk nbBuckets (Range minV maxV) =
    Vc.fromList $ L.reverse $ go [] minV nbBuckets
    where
    delta = (maxV - minV) `div` nbBuckets
    go ranges current 1 = (Range current maxV):ranges
    go ranges current n | current >= maxV = go (Range current current : ranges) current (n - 1)
    go ranges current n =
      go (Range current end' : ranges) end' (n - 1)
      where
      end' = if end > maxV then maxV else end
      end = current + delta

















