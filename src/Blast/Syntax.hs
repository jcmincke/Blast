{-# LANGUAGE RankNTypes #-}

module Blast.Syntax
where



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
     RemoteExp (Rdd a) -> Fun a b -> StateT Int m (RemoteExp (Rdd b))
rmap e f = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ RMap index key e (fmap Just f)

rflatmap :: (MonadIO m, S.Serialize a, S.Serialize b) =>
     RemoteExp (Rdd a) -> Fun a [b] -> StateT Int m (RemoteExp (Rdd b))
rflatmap e f = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ RFlatMap index key e f

rfilter :: (MonadIO m, S.Serialize a) =>
        RemoteExp (Rdd a) -> Fun a Bool -> StateT Int m (RemoteExp (Rdd a))
rfilter e p = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ RMap index key e cs
  where
  cs = fmap (\(a, bool) -> if bool then Just a else Nothing) (pass p)
  pass :: Fun a b -> Fun a (a,b)
  pass (Pure f) = Pure (\a -> (a, f a))
  pass (Closure ce f) = Closure ce (\c a -> (a, (f c) a))

collect :: (S.Serialize a, MonadIO m) =>
        RemoteExp (Rdd a) -> StateT Int m (LocalExp (Rdd a))
collect a = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ Collect index key a

count :: (S.Serialize a, Show a, MonadIO m) =>
         RemoteExp (Rdd a) -> StateT Int m (LocalExp Int)
count e = do
  zero <- lcst (0::Int)
  lfold e (foldFun (\b _ -> b+1)) zero

rcst :: (S.Serialize a, MonadIO m) => a -> StateT Int m (RemoteExp a)
rcst a = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ RConst index key a

cstRdd :: (S.Serialize a, MonadIO m) => [a] -> StateT Int m (RemoteExp (Rdd a))
cstRdd a = rcst $ Rdd a


lcst :: (S.Serialize a, MonadIO m) => a -> StateT Int m (LocalExp a)
lcst a = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ LConst index key a

join :: (Show a, S.Serialize a, S.Serialize b, MonadIO m) =>
         RemoteExp (Rdd a) -> RemoteExp (Rdd b) -> StateT Int m (RemoteExp (Rdd (a, b)))
join a b = do
  a' <- collect a
  rflatmap b (closure a' doJoin)
  where
  doJoin (Rdd ce) x = do
    c <- ce
    return (c, x)

lfold :: (Show a, Show r, S.Serialize a, S.Serialize r, MonadIO m) =>
         RemoteExp (Rdd a) -> FoldFun a r -> LocalExp r -> StateT Int m (LocalExp r)
lfold a (FoldPure f) zero = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  let av = Apply (ConstApply (\z -> ((), z))) zero
  cv <- from av
  let cs = PreparedFoldClosure cv (\() -> f)
  return $ LFold index key a cs
lfold a (FoldClosure cv f) zero = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  let av = Apply (Apply (ConstApply (\c z -> (c, z))) cv) zero
  cv' <- from av
  let cs = PreparedFoldClosure cv' f
  return $ LFold index key a cs


rfold :: (Show a, Show r, S.Serialize a, S.Serialize r, MonadIO m) =>
         RemoteExp (Rdd a) -> FoldFun a r -> LocalExp r -> StateT Int m (RemoteExp (Rdd r))
rfold a (FoldPure f) zero = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  let av = Apply (ConstApply (\z -> ((), z))) zero
  cv <- from av
  let cs = PreparedFoldClosure cv (\() -> f)
  return $ RFold index key a cs
rfold a (FoldClosure cv f) zero = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  let av = Apply (Apply (ConstApply (\c z -> (c, z))) cv) zero
  cv' <- from av
  let cs = PreparedFoldClosure cv' f
  return $ RFold index key a cs


rfold' :: (Show a, Show r, S.Serialize a, S.Serialize r, MonadIO m) =>
         RemoteExp (Rdd a) -> FoldFun a r -> ([r] -> r) -> LocalExp r -> StateT Int m (LocalExp r)
rfold' a f aggregator zero = do
  rs <- rfold a f zero
  ars <- collect rs
  lmap (fun aggregator') ars
  where
  aggregator' (Rdd x) = aggregator x


lmap :: (S.Serialize a, S.Serialize b, MonadIO m) =>
         Fun a b -> LocalExp a -> StateT Int m (LocalExp b)
lmap f e = do
  index <- get
  put (index+1)
  key <- liftIO V.newKey
  return $ LMap index key f e

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

