{-# LANGUAGE GADTs #-}

module Blast.Optimizer

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
import            Blast.Analyser



optimize :: (MonadIO m) => Int -> InfoMap -> LocalExp a -> m (InfoMap, LocalExp a)
optimize count infos e = do
    liftIO $ putStrLn "entering optimization"
    (e', (count', b)) <- runStateT (fuseLocal infos e) (count, False)
    if b
      then do liftIO $ print ("optimize: ", count')
              optimize count' infos e'
      else do infos' <- execStateT (analyseLocal e') M.empty
              liftIO $ putStrLn "exiting optimization"
              return (infos', e')


cat' :: (MonadIO m) => Fun a (Maybe b) -> Fun b (Maybe c) -> StateT (Int, Bool) m (Fun a (Maybe c))
cat' (Pure f) (Pure g) = do
  (count, _) <- get
  put (count, True)
  liftIO $ putStrLn "fuse pure-pure"
  return $ Pure (\x -> f x >>= g)

cat' (Closure c f) (Pure g) = do
  (count, _) <- get
  put (count, True)
  liftIO $ putStrLn "fuse closure-pure"
  return $ Closure c (\c a -> (f c) a >>= g)

cat' (Pure f) (Closure c g) = do
  (count, _) <- get
  put (count, True)
  liftIO $ putStrLn "fuse pure closure"
  return $ Closure c (\c a -> f a >>= (g c))

cat' (Closure cf f) (Closure cg g)  = do
  (count, b) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply' (Apply' (ConstAppl (,)) cf) cg)
  liftIO $ putStrLn "fuse closure closure"
  return $ Closure cfg (\(cf, cg) a -> (f cf) a >>= (g cg))



fuseClosure :: (MonadIO m) => InfoMap -> Fun a b -> StateT (Int, Bool) m (Fun a b)
fuseClosure infos f@(Pure _) = return f
fuseClosure infos (Closure e f) = do
  e' <- fuseLocal infos e
  return $ Closure e' f

fuseRemote :: (MonadIO m) => InfoMap -> RemoteExp a -> StateT (Int, Bool) m (RemoteExp a)
fuseRemote infos oe@(Trans ne key ie@(Trans ni _ _ _) g) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    liftIO $ print ("ref = "::String, refCountInner, ni)
    return $ Trans ne key ie' g'  -- inner expression is shared, will be cached
    where
    refCountInner = refCount ni infos

fuseRemote infos (Trans ne key (Trans _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- cat' f' g'
    fuseRemote infos $ Trans ne key e fg
fuseRemote infos oe@(Trans n key ie@(ConstRemote _ _ _) g) = do
    g' <- fuseClosure infos g
    return $ Trans n key ie g'
fuseRemote infos (Trans ne key ie@(Join _ _ _ _) g) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ Trans ne key ie' g'


fuseRemote infos e@(ConstRemote _ _ _) = return e

fuseRemote infos e@(Join ne key a b) = do
  a' <- fuseRemote infos a
  b' <- fuseRemote infos b
  return $ Join ne key a' b'

fuseLocal :: (MonadIO m) => InfoMap -> LocalExp a -> StateT (Int, Bool) m (LocalExp a)
fuseLocal infos (Fold n key e f z) = do
  e' <- fuseRemote infos e
  return $ Fold n key e' f z

fuseLocal infos (Collect n key e) = do
  e' <- fuseRemote infos e
  return $ Collect n key e'

{-
fuseLocal infos (Apply n key f e) = do
  f' <- fuseLocal infos f
  e' <- fuseLocal infos e
  return $ Apply n key f' e'
-}

fuseLocal infos (FromAppl n key e) = do
  e' <- fuseAppl infos e
  return $ FromAppl n key e'

fuseLocal infos (FMap n key f e) = do
  e' <- fuseLocal infos e
  return $ FMap n key f e'


fuseAppl :: (MonadIO m) => InfoMap -> ApplExp a -> StateT (Int, Bool) m (ApplExp a)
fuseAppl infos (Apply' f e) = do
  f' <- fuseAppl infos f
  e' <- fuseLocal infos e
  return $ Apply' f' e'
fuseAppl infos (ConstAppl e) = return (ConstAppl e)



