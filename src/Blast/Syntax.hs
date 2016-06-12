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
where

import Debug.Trace
import            Control.Applicative hiding ((<**>))
import            Control.Monad hiding (join)
import            Control.Monad.IO.Class
import            Control.Monad.Operational
import            Control.Monad.Trans.State
import            Data.Foldable
import            Data.Hashable
import qualified  Data.HashMap.Lazy as M
import qualified  Data.List as L
import            Data.Maybe (catMaybes)
import            Data.Traversable
import            Data.Vault.Strict as V
import qualified  Data.Vector as Vc
import qualified  Data.Serialize as S

import            GHC.Generics (Generic)


import            Blast.Types

{-}
class Annotation a where
  makeAnnotation :: MonadIO m => StateT s m a



instance Annotation  (Ann ()) where
  makeAnnotation = return $ Ann ()

instance Annotation  (Ann (V.Key a)) where
  makeAnnotation = do
    key <- liftIO $ V.newKey
    return $ Ann key
-}
class Joinable a b where
  join :: a -> b -> Maybe (a, b)


fun :: (a -> b) -> Fun e a b
fun f = Pure (return . f)

closure :: (S.Serialize c, Show c, ChunkableFreeVar c) => e 'Local c -> (c -> a -> b) -> Fun e a b
closure ce f = Closure ce (\c a -> return $ f c a)


foldFun :: (r -> a -> r) -> FoldFun e a r
foldFun f = FoldPure (\r a -> return $ f r a)

foldClosure :: (S.Serialize c, Show c, ChunkableFreeVar c) => e 'Local c -> (c -> r -> a -> r) -> FoldFun e a r
foldClosure ce f = FoldClosure ce (\c r a -> return $ f c r a)

funIO :: (a -> IO b) -> Fun k a b
funIO f = Pure f

closureIO :: (S.Serialize c, Show c, ChunkableFreeVar c) => e 'Local c -> (c -> a -> IO b) -> Fun e a b
closureIO ce f = Closure ce f


foldFunIO :: (r -> a -> IO r) -> FoldFun e a r
foldFunIO f = FoldPure f

foldClosureIO :: (S.Serialize c, Show c, ChunkableFreeVar c) => e 'Local c -> (c -> r -> a -> IO r) -> FoldFun e a r
foldClosureIO ce f = FoldClosure ce f

{-}
mkRMap cs e = do
  n <- nextIndex
  ann <- makeAnnotation
  return $ RMap n ann cs e
-}


rmap :: (Builder m e, Traversable t, MonadIO m) =>
        Fun e a b -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote (t b))
rmap fm e  = do
  cs <- mkRemoteClosure fm
  rapply cs e
  where
  mkRemoteClosure :: (Builder m e, Traversable t, MonadIO m) => Fun e a b -> ProgramT (Syntax m) m (ExpClosure e (t a) (t b))
  mkRemoteClosure (Pure f) = do
    ue <- lconst ()
    return $ ExpClosure ue (\() a -> mapM f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> mapM (f c) a)

rflatmap :: (Builder m e, Traversable t, MonadIO m, Monoid (t b)) =>
        Fun e a (t b) -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote (t b))
rflatmap fp e = do
  cs <- mkRemoteClosure fp
  rapply cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lconst ()
    return $ ExpClosure ue (\() a -> foldMap f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> foldMap (f c) a)

rfilter ::  (Monad m, Applicative f, Foldable t, Monoid (f a), Builder m e)
            => Fun e a Bool
            -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote (f a))
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

count ::  (Monad m, Foldable t, Builder m e)
          => e 'Local (t a) -> ProgramT (Syntax m) m (e 'Local Int)
count e = do
  zero <- lconst (0::Int)
  f <- lconst (\b _ -> b+1)
  lfold f zero e

rfold ::  (Builder m e, Traversable t, Applicative t, S.Serialize r, MonadIO m)
          => FoldFun e a r -> e 'Local r -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Remote (t r))
rfold fp zero e = do
  cs <- mkRemoteClosure fp
  rapply cs e
  where
  mkRemoteClosure (FoldPure f) = do
      cv <- (\z -> ((), z)) <$$> zero
      return $ ExpClosure cv (\((), z) a -> do
                r <- foldM f z a
                return $ pure r)
  mkRemoteClosure (FoldClosure ce f) = do
      cv <- (\c z -> (c, z)) <$$> ce <**> zero
      return $ ExpClosure cv (\(c,z) a -> do
                r <- foldM  (f c) z a
                return $ pure r)


--rfold' :: (MonadIO m, Show a, Show r, S.Serialize (t a), Chunkable (t a), S.Serialize (t r), UnChunkable (t r), S.Serialize r
---           , Applicative t, Foldable t, NodeIndexer s
 --          , ChunkableFreeVar r) =>
  --       FoldFun a r -> (t r -> r) -> LocalExp r -> RemoteExp (t a) -> StateT s m (LocalExp r)
rfold' ::  (Builder m e, Traversable t, Applicative t, S.Serialize r, MonadIO m
            , S.Serialize (t r), UnChunkable (t r))
          => FoldFun e a r -> (t r -> r) -> e 'Local r -> e 'Remote (t a) -> ProgramT (Syntax m) m (e 'Local r)
rfold' f aggregator zero a = do
  rs <- rfold f zero a
  ars <- collect rs
  aggregator <$$> ars




instance Joinable a b where
  join a b = Just (a, b)



instance (Eq k, Show k,Show a, Show b) => Joinable (KeyedVal k a) (KeyedVal k b) where
  join (KeyedVal k1 a) (KeyedVal k2 b) | k1 == k2 = Just (KeyedVal k1 a, KeyedVal k2 b)
  join (KeyedVal k1 a) (KeyedVal k2 b) = Nothing




fromList' :: (Applicative t, Foldable t, Monoid (t a)) => [a] -> t a
fromList' l = foldMap pure l


--rjoin :: (Builder m e, MonadIO m, Chunkable (t a), UnChunkable (t b),
--          ChunkableFreeVar (t a),
--          Traversable t, Applicative t
--          , Joinable a b, Monoid (t (a, b))) =>
--         e 'Remote (t a) -> e 'Remote (t b) -> ProgramT (Syntax m) m (e 'Remote (t (a, b)))
rjoin a b = do
  a' <- collect a
  let cs = ExpClosure a' (\av bv -> return $ fromList' $ catMaybes [join a b | a <- toList av, b <- toList bv])
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
    Many $ Vc.reverse $ Vc.generate nbBuckets (\i -> buckets M.! i)
    where
    buckets = L.foldl proc M.empty l
    proc bucket' kv@(KeyedVal k _) = let
      i = hash k `mod` nbBuckets
      in M.insertWith (++) i [kv] bucket'

instance (Hashable k) => UnChunkable [KeyedVal k v] where
  unChunk l = L.concat l


instance (Applicative t, Foldable t, Monoid (t (KeyedVal k v)), Chunkable (t (KeyedVal k v))) => ChunkableFreeVar (OptiT t k v) where
  chunk' n (OptiT tkvs) = trace ("here") $ fmap OptiT $ chunk n tkvs




rKeyedJoin a b = do
  a' <- collect a
  ja <- OptiT <$$> a'
  let cs = ExpClosure ja (\(OptiT av) bv -> trace (show av) $ trace (show bv) $ return $ fromList' $ catMaybes [doJoin a b | a <- toList av, b <- toList bv])
  rapply cs b
  where
  doJoin (KeyedVal k1 a) (KeyedVal k2 b) | k1 == k2 = Just $ KeyedVal k1 (a, b)
  doJoin (KeyedVal k1 a) (KeyedVal k2 b) = Nothing





data Range = Range Int Int
  deriving (Eq, Show, Generic, S.Serialize)



instance Chunkable Range where
  chunk nbBuckets (Range min max) =
    Many $ Vc.fromList $ L.reverse $ go [] min nbBuckets
    where
    delta = (max - min) `div` nbBuckets
    go ranges current 1 = (Range current max):ranges
    go ranges current n | current >= max = go (Range current current : ranges) current (n - 1)
    go ranges current n =
      go (Range current end' : ranges) end' (n - 1)
      where
      end' = if end > max then max else end
      end = current + delta


























