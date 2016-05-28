{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}

module Blast.Syntax
where

import            Control.Monad hiding (join)
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import            Data.Traversable
import            Data.Vault.Strict as V
import qualified  Data.Serialize as S

import            Blast.Types



class Joinable a b where
  type JoinedVal a b :: *
  join :: a -> b -> JoinedVal a b


fun :: (a -> b) -> Fun a b
fun f = Pure (return . f)

closure :: forall a b c. (S.Serialize c, Show c) => LocalExp c -> (c -> a -> b) -> Fun a b
closure ce f = Closure ce (\c a -> return $ f c a)


foldFun :: (r -> a -> r) -> FoldFun a r
foldFun f = FoldPure (\r a -> return $ f r a)

foldClosure :: forall a r c. (S.Serialize c, Show c) => LocalExp c -> (c -> r -> a -> r) -> FoldFun a r
foldClosure ce f = FoldClosure ce (\c r a -> return $ f c r a)

funIO :: (a -> IO b) -> Fun a b
funIO f = Pure f

closureIO :: forall a b c. (S.Serialize c, Show c) => LocalExp c -> (c -> a -> IO b) -> Fun a b
closureIO ce f = Closure ce f


foldFunIO :: (r -> a -> IO r) -> FoldFun a r
foldFunIO f = FoldPure f

foldClosureIO :: forall a r c. (S.Serialize c, Show c) => LocalExp c -> (c -> r -> a -> IO r) -> FoldFun a r
foldClosureIO ce f = FoldClosure ce f


rmap :: (MonadIO m, S.Serialize (t a), Chunkable (t a), S.Serialize (t b), Chunkable (t b), Traversable t, NodeIndexer s) =>
      Fun a b -> RemoteExp (t a) -> StateT s m (RemoteExp (t b))
rmap fm e  = do
  index <- nextIndex
  key <- liftIO V.newKey
  cs <- mkRemoteClosure fm
  return $ RMap index key cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> mapM f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> mapM (f c) a)


rflatmap :: (MonadIO m, S.Serialize (t a), Chunkable (t a), S.Serialize (t b), Chunkable (t b), Foldable t, Monoid (t b), NodeIndexer s) =>
     Fun a (t b) -> RemoteExp (t a) -> StateT s m (RemoteExp (t b))
rflatmap fp e = do
  index <- nextIndex
  key <- liftIO V.newKey
  cs <- mkRemoteClosure fp
  return $ RMap index key cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() a -> foldMap f a)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c a -> foldMap (f c) a)

rfilter :: (MonadIO m, S.Serialize (t a), Chunkable (t a), Applicative t, Foldable t, Monoid (t a), NodeIndexer s) =>
        Fun a Bool -> RemoteExp (t a) -> StateT s m (RemoteExp (t a))
rfilter p e = do
  index <- nextIndex
  key <- liftIO V.newKey
  cs <- mkRemoteClosure p
  return $ RMap index key cs e
  where
  mkRemoteClosure (Pure f) = do
    ue <- lcst ()
    return $ ExpClosure ue (\() ta -> do
        r <- foldMap (\a -> do
            b <- f a
            return $ if b then pure a else mempty) ta
        return r) -- $ foldMap id r)
  mkRemoteClosure (Closure ce f) = return $ ExpClosure ce (\c ta -> do
        r <- foldMap (\a -> do
              b <- f c a
              return $ if b then pure a else mempty) ta
        return r) -- $ foldMap id r)


collect :: (MonadIO m, S.Serialize a, Chunkable a, NodeIndexer s) =>
        RemoteExp a -> StateT s m (LocalExp a)
collect a = do
  index <- nextIndex
  key <- liftIO V.newKey
  return $ Collect index key a

count :: (Show a, Foldable t, MonadIO m, NodeIndexer s) =>
         LocalExp (t a) -> StateT s m (LocalExp Int)
count e = do
  zero <- lcst (0::Int)
  f <- lcst (\b _ -> b+1)
  lfold f zero e

rcst :: (S.Serialize a, Chunkable a, MonadIO m, NodeIndexer s) => a -> StateT s m (RemoteExp a)
rcst a = do
  index <- nextIndex
  key <- liftIO V.newKey
  return $ RConst index key a


lcst :: (MonadIO m, NodeIndexer s) => a -> StateT s m (LocalExp a)
lcst a = do
  index <- nextIndex
  key <- liftIO V.newKey
  return $ LConst index key a


rjoin :: (MonadIO m, Show a, S.Serialize a, S.Serialize b, S.Serialize (JoinedVal a b),
          Joinable a b, Chunkable a, Chunkable b, Chunkable (JoinedVal a b), NodeIndexer s) =>
         RemoteExp a -> RemoteExp b -> StateT s m (RemoteExp (JoinedVal a b))
rjoin a b = do
  a' <- collect a
  index <- nextIndex
  key <- liftIO V.newKey
  let cs = ExpClosure a' (\av bv -> return $ join av bv)
  return $ RMap index key cs b


lfold :: (MonadIO m, Show a, Show r, Foldable t, NodeIndexer s) =>
         LocalExp (r -> a -> r) -> LocalExp r -> LocalExp (t a) -> StateT s m (LocalExp r)
lfold f zero a = do
  index <- nextIndex
  key <- liftIO V.newKey
  f' <- foldl <$$> f <**> zero
  return $ FMap index key f' a


lfold' :: (MonadIO m, Show a, Show r, Foldable t, NodeIndexer s) =>
         (r -> a -> r) -> LocalExp r -> LocalExp (t a) -> StateT s m (LocalExp r)
lfold' f zero a = do
  f' <- lcst f
  lfold f' zero a

rfold :: (MonadIO m, Show a, Show r, S.Serialize (t a), Chunkable (t a), S.Serialize (t r), Chunkable (t r)
          , S.Serialize r, Applicative t, Foldable t, NodeIndexer s) =>
         FoldFun a r -> LocalExp r -> RemoteExp (t a) -> StateT s m (RemoteExp (t r))
rfold fp zero e = do
  index <- nextIndex
  key <- liftIO V.newKey
  cs <- mkRemoteClosure fp
  return $ RMap index key cs e
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


rfold' :: (MonadIO m, Show a, Show r, S.Serialize (t a), Chunkable (t a), S.Serialize (t r), Chunkable (t r), S.Serialize r
           , Applicative t, Foldable t, NodeIndexer s) =>
         FoldFun a r -> (t r -> r) -> LocalExp r -> RemoteExp (t a) -> StateT s m (LocalExp r)
rfold' f aggregator zero a = do
  rs <- rfold f zero a
  ars <- collect rs
  aggregator <$$> ars


(<**>) :: (MonadIO m, NodeIndexer s) => StateT s m (LocalExp (a->b)) -> LocalExp a -> StateT s m (LocalExp b)
f <**> a = do
  cs <- f
  index <- nextIndex
  key <- liftIO V.newKey
  return $ FMap index key cs a


(<$$>) :: (MonadIO m, NodeIndexer s) => (a->b) -> LocalExp a -> StateT s m (LocalExp b)
f <$$> e = lcst f <**> e
