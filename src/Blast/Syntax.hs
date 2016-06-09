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


class Annotation a where
  makeAnnotation :: MonadIO m => StateT s m a



instance Annotation  (Ann ()) where
  makeAnnotation = return $ Ann ()

instance Annotation  (Ann (V.Key a)) where
  makeAnnotation = do
    key <- liftIO $ V.newKey
    return $ Ann key

class Joinable a b where
  join :: a -> b -> Maybe (a, b)


fun :: (a -> b) -> Fun k a b
fun f = Pure (return . f)

closure :: forall k a b c. (S.Serialize c, Show c, ChunkableFreeVar c) => LocalExp k c -> (c -> a -> b) -> Fun k a b
closure ce f = Closure ce (\c a -> return $ f c a)


foldFun :: (r -> a -> r) -> FoldFun k a r
foldFun f = FoldPure (\r a -> return $ f r a)

foldClosure :: forall a r c k. (S.Serialize c, Show c, ChunkableFreeVar c) => LocalExp k c -> (c -> r -> a -> r) -> FoldFun k a r
foldClosure ce f = FoldClosure ce (\c r a -> return $ f c r a)

funIO :: (a -> IO b) -> Fun k a b
funIO f = Pure f

closureIO :: forall a b c k. (S.Serialize c, Show c, ChunkableFreeVar c) => LocalExp k c -> (c -> a -> IO b) -> Fun k a b
closureIO ce f = Closure ce f


foldFunIO :: (r -> a -> IO r) -> FoldFun k a r
foldFunIO f = FoldPure f

foldClosureIO :: forall a r c (k::Kind). (S.Serialize c, Show c, ChunkableFreeVar c) => LocalExp k c -> (c -> r -> a -> IO r) -> FoldFun k a r
foldClosureIO ce f = FoldClosure ce f

mkRMap cs e = do
  n <- nextIndex
  ann <- makeAnnotation
  return $ RMap n ann cs e

rmap :: forall m t a b s k. (MonadIO m, S.Serialize (t a), Chunkable (t a)
      , S.Serialize (t b), UnChunkable (t b), Traversable t, NodeIndexer s
      , Annotation (RMapAnn k (t b))
      , Annotation (LConstAnn k ())) =>
      Fun k a b -> RemoteExp k (t a) -> StateT s m (RemoteExp k (t b))
rmap fm e  = do
  n <- nextIndex
  ann <- makeAnnotation
  cs <- mkRemoteClosure fm
  return $ RMap n ann cs e
  where
  mkRemoteClosure :: (Annotation (LConstAnn k ())) => Fun k a b -> StateT s m (ExpClosure k (t a) (t b))
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> mapM f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> mapM (f c) a)



lcst :: forall m s k ctor a. (MonadIO m, NodeIndexer s, Annotation (LConstAnn k a)) => a -> StateT s m (LocalExp k a)
lcst a = do
  n <- nextIndex
  ann <- makeAnnotation
  return $ LConst n ann a

{-}
rflatmap :: (MonadIO m, S.Serialize (t a), Chunkable (t a), S.Serialize (t b), UnChunkable (t b), Foldable t, Monoid (t b), NodeIndexer s) =>
     Fun a (t b) -> RemoteExp (t a) -> StateT s m (RemoteExp (t b))
rflatmap fp e = do
  ann <- makeAnnotation
  cs <- mkRemoteClosure fp
  return $ RMap ann cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> foldMap f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> foldMap (f c) a)

rfilter :: (MonadIO m, S.Serialize (t a), Chunkable (t a), UnChunkable (t a), Applicative t, Foldable t, Monoid (t a), NodeIndexer s) =>
        Fun a Bool -> RemoteExp (t a) -> StateT s m (RemoteExp (t a))
rfilter p e = do
  ann <- makeAnnotation
  cs <- mkRemoteClosure p
  return $ RMap ann cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
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


collect :: (MonadIO m, S.Serialize a, UnChunkable a, NodeIndexer s) =>
        RemoteExp a -> StateT s m (LocalExp a)
collect a = do
  ann <- makeAnnotation
  return $ Collect ann a

count :: (Show a, Foldable t, MonadIO m, NodeIndexer s) =>
         LocalExp (t a) -> StateT s m (LocalExp Int)
count e = do
  zero <- lcst (0::Int)
  f <- lcst (\b _ -> b+1)
  lfold f zero e


rcst :: (S.Serialize a, Chunkable a, MonadIO m, NodeIndexer s, Annotation s (RAnnotation k 'RConstCtor a)) => a -> StateT s m (RemoteExp k a)
rcst a = do
  ann <- makeAnnotation
  return $ RConst ann a




lfold :: (MonadIO m, Show a, Show r, Foldable t, NodeIndexer s) =>
         LocalExp (r -> a -> r) -> LocalExp r -> LocalExp (t a) -> StateT s m (LocalExp r)
lfold f zero a = do
  ann <- makeAnnotation
  f' <- foldl <$$> f <**> zero
  return $ FMap ann f' a


lfold' :: (MonadIO m, Show a, Show r, Foldable t, NodeIndexer s) =>
         (r -> a -> r) -> LocalExp r -> LocalExp (t a) -> StateT s m (LocalExp r)
lfold' f zero a = do
  f' <- lcst f
  lfold f' zero a

rfold :: (MonadIO m, Show a, Show r, S.Serialize (t a), Chunkable (t a), S.Serialize (t r), UnChunkable (t r)
          , S.Serialize r, Applicative t, Foldable t, NodeIndexer s
          , ChunkableFreeVar r) =>
         FoldFun a r -> LocalExp r -> RemoteExp (t a) -> StateT s m (RemoteExp (t r))
rfold fp zero e = do
  ann <- makeAnnotation
  cs <- mkRemoteClosure fp
  return $ RMap ann cs e
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


rfold' :: (MonadIO m, Show a, Show r, S.Serialize (t a), Chunkable (t a), S.Serialize (t r), UnChunkable (t r), S.Serialize r
           , Applicative t, Foldable t, NodeIndexer s
           , ChunkableFreeVar r) =>
         FoldFun a r -> (t r -> r) -> LocalExp r -> RemoteExp (t a) -> StateT s m (LocalExp r)
rfold' f aggregator zero a = do
  rs <- rfold f zero a
  ars <- collect rs
  aggregator <$$> ars


(<**>) :: (MonadIO m, NodeIndexer s) => StateT s m (LocalExp (a->b)) -> LocalExp a -> StateT s m (LocalExp b)
f <**> a = do
  cs <- f
  ann <- makeAnnotation
  return $ FMap ann cs a


(<$$>) :: (MonadIO m, NodeIndexer s) => (a->b) -> LocalExp a -> StateT s m (LocalExp b)
f <$$> e = lcst f <**> e



instance (Show a , Show b) => Joinable a b where
  join a b = Just (a, b)

instance (Eq k, Show k,Show a, Show b) => Joinable (KeyedVal k a) (KeyedVal k b) where
  join (KeyedVal k1 a) (KeyedVal k2 b) | k1 == k2 = Just (KeyedVal k1 a, KeyedVal k2 b)
  join (KeyedVal k1 a) (KeyedVal k2 b) = Nothing

fromList' :: (Applicative t, Foldable t, Monoid (t a)) => [a] -> t a
fromList' l = foldMap pure l


rjoin :: (MonadIO m, Show (t a), S.Serialize (t a), S.Serialize (t b), S.Serialize (t (a, b)), Traversable t, Applicative t
          , Joinable a b, UnChunkable (t a), Chunkable (t b), UnChunkable (t (a, b)), Monoid (t (a, b)), NodeIndexer s
          , ChunkableFreeVar (t a)) =>
         RemoteExp (t a) -> RemoteExp (t b) -> StateT s m (RemoteExp (t (a, b)))
rjoin a b = do
  a' <- collect a
  ann <- makeAnnotation
  let cs = ExpClosure a' (\av bv -> return $ fromList' $ catMaybes [join a b | a <- toList av, b <- toList bv])
  return $ RMap ann cs b

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



rKeyedJoin :: (MonadIO m, Eq k, Show k, Show (t (KeyedVal k a)), S.Serialize (t (KeyedVal k a)), S.Serialize(t (KeyedVal k b)), S.Serialize (t (KeyedVal k (a, b))), Traversable t, Applicative t
          , Joinable (KeyedVal k a) (KeyedVal k b)
          , UnChunkable (t (KeyedVal k a)), Chunkable (t (KeyedVal k b))
          , Chunkable (t (KeyedVal k a))
          , UnChunkable (t (KeyedVal k (a, b)))
          , Monoid (t (KeyedVal k (a, b))), NodeIndexer s
          , ChunkableFreeVar (t (KeyedVal k a))
          , Show (t (KeyedVal k b))
          , Show (OptiT t k a)
          , S.Serialize (OptiT t k a)
          , Monoid (t (KeyedVal k a))
          ) =>
         RemoteExp (t (KeyedVal k a)) -> RemoteExp (t (KeyedVal k b)) -> StateT s m (RemoteExp (t (KeyedVal k (a, b))))
rKeyedJoin a b = do
  a' <- collect a
  ja <- OptiT <$$> a'
  ann <- makeAnnotation
  let cs = ExpClosure ja (\(OptiT av) bv -> trace (show av) $ trace (show bv) $ return $ fromList' $ catMaybes [doJoin a b | a <- toList av, b <- toList bv])
  return $ RMap ann cs b
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








-}





















