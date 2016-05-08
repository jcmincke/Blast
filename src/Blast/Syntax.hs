{-# LANGUAGE RankNTypes #-}

module Blast.Syntax
where


import Debug.Trace

import qualified  Data.Vault.Strict as V
import            Control.Bool (unlessM)
import            Control.Lens (makeLenses, set, view)
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import            Data.IORef
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe
import qualified  Data.Serialize as S
import            GHC.Generics (Generic)

import            Blast.Types




pass :: Fun a b -> Fun a (a,b)
pass (Pure f) = Pure (\a -> (a, f a))
pass (Closure c f) = Closure c (\c a -> (a, (f c) a))



fun :: (a->b) -> Fun a b
fun f = Pure f

closure :: forall a b c. (S.Serialize c, Show c) => LocalExp c -> (c -> a -> b) -> Fun a b
closure c f = Closure c f




smap :: (MonadIO m, S.Serialize a, S.Serialize b) =>
     RemoteExp (Rdd a) -> Fun a b -> StateT Int m (RemoteExp (Rdd b))
smap e f = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ Trans c key e (fmap Just f)

sfilter :: (MonadIO m, S.Serialize a) =>
        RemoteExp (Rdd a) -> Fun a Bool -> StateT Int m (RemoteExp (Rdd a))
sfilter e p = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ Trans c key e cs
  where
  cs = fmap (\(a, bool) -> if bool then Just a else Nothing) (pass p)

collect :: (S.Serialize a, MonadIO m) =>
        RemoteExp (Rdd a) -> StateT Int m (LocalExp (Rdd a))
collect a = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ Collect c key a

count :: (S.Serialize a, MonadIO m) =>
         RemoteExp (Rdd a) -> StateT Int m (LocalExp Int)
count e = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ Fold c key e (\b _ -> b+1) (0::Int)

cstRemote :: (S.Serialize a, MonadIO m) => a -> StateT Int m (RemoteExp a)
cstRemote a = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ ConstRemote c key a

cstRdd :: (S.Serialize a, MonadIO m) => [a] -> StateT Int m (RemoteExp (Rdd a))
cstRdd a = cstRemote $ Rdd a

sjoin :: (S.Serialize a, S.Serialize b, MonadIO m) =>
         RemoteExp (Rdd a) -> RemoteExp (Rdd b) -> StateT Int m (RemoteExp (Rdd (a, b)))
sjoin a b = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ Join c key a b

sfmap :: (S.Serialize a, S.Serialize b, MonadIO m) =>
         (a->b) -> LocalExp a -> StateT Int m (LocalExp b)
sfmap f e = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ FMap c key f e

(<**>) :: (S.Serialize a) => ApplExp (a->b) -> LocalExp a -> ApplExp b
f <**> e = Apply' f e

(<$$>) :: (S.Serialize a) => (a->b) -> LocalExp a -> ApplExp b
f <$$> e = Apply' (ConstAppl f) e

sfrom :: (S.Serialize a, MonadIO m) =>
         ApplExp a -> StateT Int m (LocalExp a)
sfrom e = do
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ FromAppl c key e


{-
(<**>) :: (S.Serialise b, MonadIO m) =>
       StateT Int m (LocalExp (a->b)) -> LocalExp a -> StateT Int m (LocalExp b)
fm <**> e = do
  f <- fm
  c <- get
  put (c+1)
  key <- liftIO V.newKey
  return $ Apply c key f e

(<$$>) :: MonadIO m =>
       (a->b) -> LocalExp a -> StateT Int m (LocalExp b)
f <$$> e = sfmap f e
-}
