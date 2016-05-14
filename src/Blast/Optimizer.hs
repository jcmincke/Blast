{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Optimizer

where


import qualified  Data.Vault.Strict as V
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Map as M
import            Data.Maybe
import qualified  Data.Text as T

import            Blast.Types
import            Blast.Analyser



optimize :: (MonadLoggerIO m) => Int -> InfoMap -> LocalExp a -> m (InfoMap, LocalExp a)
optimize = do
    go (1::Int)
    where
    go nbIter count infos e = do
      (e', (count', b)) <- runStateT (fuseLocal infos e) (count, False)
      $(logInfo) $ T.pack ("Optimization: iteration nb " ++ show nbIter)
      if b
        then do go (nbIter+1) count' infos e'
        else do infos' <- execStateT (analyseLocal e') M.empty
                $(logInfo) $ T.pack ("Exiting optimization after "++ show nbIter ++ " iteration(s)")
                return (infos', e')

optimized :: (MonadLoggerIO m) => StateT (Int, Bool) m ()
optimized = do
  (count, _) <- get
  put (count, True)

combineClosure :: (MonadLoggerIO m, Monad mi) => Fun a (mi b) -> Fun b (mi c) -> StateT (Int, Bool) m (Fun a (mi c))
combineClosure (Pure f) (Pure g) = do
  optimized
  return $ Pure (\x -> f x >>= g)
combineClosure (Closure ce f) (Pure g) = do
  optimized
  return $ Closure ce (\c a -> (f c) a >>= g)
combineClosure (Pure f) (Closure ce g) = do
  optimized
  return $ Closure ce (\c a -> f a >>= (g c))
combineClosure (Closure cf f) (Closure cg g)  = do
  (count, _) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply' (Apply' (ConstAppl (,)) cf) cg)
  return $ Closure cfg (\(cf', cg') a -> (f cf') a >>= (g cg'))


combineClosureF :: (MonadLoggerIO m) => Fun a [b] -> Fun b (Maybe c) -> StateT (Int, Bool) m (Fun a [c])
combineClosureF (Pure f) (Pure g) = do
  optimized
  return $ Pure (\a -> mapMaybe g (f a))
combineClosureF (Closure c f) (Pure g) = do
  optimized
  return $ Closure c (\c' a -> mapMaybe g ((f c') a))
combineClosureF (Pure f) (Closure c g) = do
  optimized
  return $ Closure c (\c' a -> mapMaybe (g c') (f a))
combineClosureF (Closure cf f) (Closure cg g)  = do
  (count, _) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply' (Apply' (ConstAppl (,)) cf) cg)
  liftIO $ putStrLn "fuse fclosure closure"
  return $ Closure cfg (\(cf', cg') a -> mapMaybe (g cg') ((f cf') a))


combineClosureF' :: (MonadLoggerIO m) => Fun a (Maybe b) -> Fun b [c]  -> StateT (Int, Bool) m (Fun a [c])
combineClosureF' (Pure f) (Pure g) = do
  optimized
  return $ Pure (\a -> maybe [] g (f a))

combineClosureF' (Closure c f) (Pure g) = do
  optimized
  return $ Closure c (\c' a -> maybe [] g ((f c') a))

combineClosureF' (Pure f) (Closure c g) = do
  optimized
  return $ Closure c (\c' a -> maybe [] (g c') (f a))

combineClosureF' (Closure cf f) (Closure cg g)  = do
  (count, _) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply' (Apply' (ConstAppl (,)) cf) cg)
  liftIO $ putStrLn "fuse closure fclosure"
  return $ Closure cfg (\(cf', cg') a -> maybe [] (g cg') ((f cf') a))


fuseClosure :: (MonadLoggerIO m) => InfoMap -> Fun a b -> StateT (Int, Bool) m (Fun a b)
fuseClosure _ f@(Pure _) = return f
fuseClosure infos (Closure e f) = do
  e' <- fuseLocal infos e
  return $ Closure e' f

fuseRemote :: (MonadLoggerIO m) => InfoMap -> RemoteExp a -> StateT (Int, Bool) m (RemoteExp a)
fuseRemote infos (Map ne key ie g) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    --liftIO $ print ("ref = "::String, refCountInner, (getRemoteIndex ie))
    return $ Map ne key ie' g'  -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (Map ne key (Map _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    fuseRemote infos $ Map ne key e fg

fuseRemote infos (Map ne key (FlatMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosureF f' g'
    fuseRemote infos $ FlatMap ne key e fg

fuseRemote infos (Map n key ie@(ConstRemote _ _ _) g) = do
    g' <- fuseClosure infos g
    return $ Map n key ie g'

fuseRemote infos (FlatMap ne key ie g) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    --liftIO $ print ("ref = "::String, refCountInner, (getRemoteIndex ie))
    return $ FlatMap ne key ie' g'  -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (FlatMap ne key (Map _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosureF' f' g'
    fuseRemote infos $ FlatMap ne key e fg

fuseRemote infos (FlatMap ne key (FlatMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    fuseRemote infos $ FlatMap ne key e fg

fuseRemote infos (FlatMap n key ie@(ConstRemote _ _ _) g) = do
    g' <- fuseClosure infos g
    return $ FlatMap n key ie g'


fuseRemote _ e@(ConstRemote _ _ _) = return e


fuseLocal :: (MonadLoggerIO m) => InfoMap -> LocalExp a -> StateT (Int, Bool) m (LocalExp a)
fuseLocal infos (Fold n key e f z) = do
  e' <- fuseRemote infos e
  return $ Fold n key e' f z

fuseLocal infos (Collect n key e) = do
  e' <- fuseRemote infos e
  return $ Collect n key e'

fuseLocal infos (FromAppl n key e) = do
  e' <- fuseAppl infos e
  return $ FromAppl n key e'

fuseLocal infos (FMap n key f e) = do
  e' <- fuseLocal infos e
  return $ FMap n key f e'

fuseLocal _ e@(ConstLocal _ _ _) = return e


fuseAppl :: (MonadLoggerIO m) => InfoMap -> ApplExp a -> StateT (Int, Bool) m (ApplExp a)
fuseAppl infos (Apply' f e) = do
  f' <- fuseAppl infos f
  e' <- fuseLocal infos e
  return $ Apply' f' e'
fuseAppl _ (ConstAppl e) = return (ConstAppl e)



