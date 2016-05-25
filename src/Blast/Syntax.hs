{-# LANGUAGE RankNTypes #-}

module Blast.Syntax
where


import qualified  Data.List as L
import qualified  Data.Vault.Strict as V
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import qualified  Data.Serialize as S

import            Blast.Types



fun :: (a -> b) -> Fun a b
fun f = Pure f

closure :: forall a b c. (S.Serialize c, Show c) => LocalExp c -> (c -> a -> b) -> Fun a b
closure c f = Closure c f


foldFun :: (r -> a -> r) -> FoldFun a r
foldFun f = FoldPure f

foldClosure :: forall a r c. (S.Serialize c, Show c) => LocalExp c -> (c -> r -> a -> r) -> FoldFun a r
foldClosure c f = FoldClosure c f


rmap :: (MonadIO m, S.Serialize a, S.Serialize b) =>
     RemoteExp [a] -> Fun a b -> StateT Int m (RemoteExp [b])
rmap e f = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  cs <- mkRemoteClosure f
  return $ RMap index key e cs
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> L.map f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> L.map (f c) a)

rflatmap :: (MonadIO m, S.Serialize a, S.Serialize b) =>
     RemoteExp [a] -> Fun a [b] -> StateT Int m (RemoteExp [b])
rflatmap e f = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  cs <- mkRemoteClosure f
  return $ RMap index key e cs
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> L.concat $ L.map f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> L.concat $ L.map (f c) a)

rfilter :: (MonadIO m, S.Serialize a) =>
        RemoteExp [a] -> Fun a Bool -> StateT Int m (RemoteExp [a])
rfilter e p = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  cs <- mkRemoteClosure p
  return $ RMap index key e cs
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> L.filter f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> L.filter (f c) a)

  cs = fmap (\(a, bool) -> if bool then Just a else Nothing) (pass p)
  pass :: Fun a b -> Fun a (a,b)
  pass (Pure f) = Pure (\a -> (a, f a))
  pass (Closure ce f) = Closure ce (\c a -> (a, (f c) a))

collect :: (S.Serialize a, MonadIO m) =>
        RemoteExp [a] -> StateT Int m (LocalExp [a])
collect a = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ Collect index key a

count :: (S.Serialize a, Show a, MonadIO m) =>
         LocalExp [a] -> StateT Int m (LocalExp Int)
count e = do
  zero <- lcst (0::Int)
  lfold e (foldFun (\b _ -> b+1)) zero

rcst :: (S.Serialize a, Chunkable a, MonadIO m) => a -> StateT Int m (RemoteExp a)
rcst a = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ RConst index key a


lcst :: (S.Serialize a, MonadIO m) => a -> StateT Int m (LocalExp a)
lcst a = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ LConst index key a

join :: (Show a, S.Serialize a, S.Serialize b, MonadIO m) =>
         RemoteExp [a] -> RemoteExp [b] -> StateT Int m (RemoteExp [(a, b)])
join a b = do
  a' <- collect a
  rflatmap b (closure a' doJoin)
  where
  doJoin ce x = do
    c <- ce
    return (c, x)

lfold :: (Show a, Show r, S.Serialize a, S.Serialize r, MonadIO m) =>
         LocalExp [a] -> FoldFun a r -> LocalExp r -> StateT Int m (LocalExp r)
lfold a f zero = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  cs <- mkRemoteClosure f
  return $ LMap index key a cs
  where
  mkRemoteClosure (FoldPure f) = do
      let av = Apply (ConstApply (\z -> ((), z))) zero
      cv <- from av
      return $ ExpClosure cv (\((), z) a -> L.foldl' f z a)
  mkRemoteClosure (FoldClosure ce f) = do
      let av = Apply (Apply (ConstApply (\c z -> (c, z))) ce) zero
      cv <- from av
      return $ ExpClosure cv (\(c,z) a -> L.foldl' (f c) z a)


rfold :: (Show a, Show r, S.Serialize a, S.Serialize r, MonadIO m) =>
         RemoteExp [a] -> FoldFun a r -> LocalExp r -> StateT Int m (RemoteExp [r])
rfold a f zero = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  cs <- mkRemoteClosure f
  return $ RMap index key a cs
  where
  mkRemoteClosure (FoldPure f) = do
      let av = Apply (ConstApply (\z -> ((), z))) zero
      cv <- from av
      return $ ExpClosure cv (\((), z) a -> [L.foldl' f z a])
  mkRemoteClosure (FoldClosure ce f) = do
      let av = Apply (Apply (ConstApply (\c z -> (c, z))) ce) zero
      cv <- from av
      return $ ExpClosure cv (\(c,z) a -> [L.foldl' (f c) z a])


rfold' :: (Show a, Show r, S.Serialize a, S.Serialize r, MonadIO m) =>
         RemoteExp [a] -> FoldFun a r -> ([r] -> r) -> LocalExp r -> StateT Int m (LocalExp r)
rfold' a f aggregator zero = do
  rs <- rfold a f zero
  ars <- collect rs
  lmap (fun aggregator') ars
  where
  aggregator' x = aggregator x


lmap :: (S.Serialize a, S.Serialize b, MonadIO m) =>
         Fun a b -> LocalExp a -> StateT Int m (LocalExp b)
lmap f e = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  cs <- mkRemoteClosure f
  return $ LMap index key e cs
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> (f c) a)

(<**>) :: (S.Serialize a) => ApplyExp (a->b) -> LocalExp a -> ApplyExp b
f <**> e = Apply f e

(<$$>) :: (S.Serialize a) => (a->b) -> LocalExp a -> ApplyExp b
f <$$> e = Apply (ConstApply f) e

from :: (S.Serialize a, MonadIO m) =>
         ApplyExp a -> StateT Int m (LocalExp a)
from e = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ FromAppl index  key e

