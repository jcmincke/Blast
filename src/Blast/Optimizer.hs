{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Optimizer

where

--import Debug.Trace
import qualified  Data.List as L
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
      $(logInfo) $ T.pack ("Start Optimization: iteration nb " ++ show nbIter)
      (e', (count', b)) <- runStateT (fuseLocal infos e) (count, False)
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
  let cfg = FromAppl count lc (Apply (Apply (ConstApply (,)) cf) cg)
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
  let cfg = FromAppl count lc (Apply (Apply (ConstApply (,)) cf) cg)
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
  let cfg = FromAppl count lc (Apply (Apply (ConstApply (,)) cf) cg)
  liftIO $ putStrLn "fuse closure fclosure"
  return $ Closure cfg (\(cf', cg') a -> maybe [] (g cg') ((f cf') a))



combineFoldClosure :: (MonadLoggerIO m) =>
  Fun a (Maybe b) -> PreparedFoldClosure b r -> StateT (Int, Bool) m (PreparedFoldClosure a r)
combineFoldClosure (Pure f) (PreparedFoldClosure ce g) = do
  optimized
  return $ PreparedFoldClosure ce (\c r a ->
      case f a of
        Just b -> (g c) r b
        Nothing -> r)
combineFoldClosure (Closure cf f) (PreparedFoldClosure cg g) = do
  (count, _) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply (Apply (ConstApply (\cf' (cg', z) -> ((cf', cg'), z))) cf) cg)
  return $ PreparedFoldClosure cfg (\(cf', cg') r a ->
      case (f cf') a of
        Just b -> (g cg') r b
        Nothing -> r)

combineFoldClosureF :: (MonadLoggerIO m) =>
  Fun a [b] -> PreparedFoldClosure b r -> StateT (Int, Bool) m (PreparedFoldClosure a r)
combineFoldClosureF (Pure f) (PreparedFoldClosure ce g) = do
  optimized
  return $ PreparedFoldClosure ce (\c r a -> let
      bs = f a
      in L.foldl' (g c) r bs
      )
combineFoldClosureF (Closure cf f) (PreparedFoldClosure cg g) = do
  (count, _) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply (Apply (ConstApply (\cf' (cg', z) -> ((cf', cg'), z))) cf) cg)
  return $ PreparedFoldClosure cfg (\(cf', cg') r a ->
      let bs = (f cf') a
      in L.foldl' (g cg') r bs
      )


fuseClosure :: (MonadLoggerIO m) => InfoMap -> Fun a b -> StateT (Int, Bool) m (Fun a b)
fuseClosure _ f@(Pure _) = return f
fuseClosure infos (Closure e f) = do
  e' <- fuseLocal infos e
  return $ Closure e' f


fuseRemoteFoldClosure :: (MonadLoggerIO m) => InfoMap -> PreparedFoldClosure a r -> StateT (Int, Bool) m (PreparedFoldClosure a r)
fuseRemoteFoldClosure infos (PreparedFoldClosure e f) = do
  e' <- fuseLocal infos e
  return $  PreparedFoldClosure e' f

fuseRemote :: (MonadLoggerIO m) => InfoMap -> RemoteExp a -> StateT (Int, Bool) m (RemoteExp a)
fuseRemote infos (RMap ne key ie g) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    --liftIO $ print ("ref = "::String, refCountInner, (getRemoteIndex ie))
    return $ RMap ne key ie' g'  -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (RMap ne key (RMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    fuseRemote infos $ RMap ne key e fg

fuseRemote infos (RMap ne key (RFlatMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosureF f' g'
    fuseRemote infos $ RFlatMap ne key e fg


fuseRemote infos (RMap n key ie g) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ RMap n key ie' g'


fuseRemote infos (RFold ne key ie f)  | refCountInner > 1 = do
    f' <- fuseRemoteFoldClosure infos f
    ie' <- fuseRemote infos ie
    return $ RFold ne key ie' f'
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (RFold ne key (RMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseRemoteFoldClosure infos g
    fg <- combineFoldClosure f' g'
    fuseRemote infos $ RFold ne key e fg

fuseRemote infos (RFold ne key (RFlatMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseRemoteFoldClosure infos g
    fg <- combineFoldClosureF f' g'
    fuseRemote infos $ RFold ne key e fg

fuseRemote infos (RFold ne key ie f) = do
    f' <- fuseRemoteFoldClosure infos f
    ie' <- fuseRemote infos ie
    return $ RFold ne key ie' f'




fuseRemote infos (RFlatMap ne key ie g) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    --liftIO $ print ("ref = "::String, refCountInner, (getRemoteIndex ie))
    return $ RFlatMap ne key ie' g'  -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (RFlatMap ne key (RMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosureF' f' g'
    fuseRemote infos $ RFlatMap ne key e fg

fuseRemote infos (RFlatMap ne key (RFlatMap _ _ e f) g) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    fuseRemote infos $ RFlatMap ne key e fg

fuseRemote infos (RFlatMap n key ie g) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ RFlatMap n key ie' g'


fuseRemote _ e@(RConst _ _ _) = return e


fuseLocal :: (MonadLoggerIO m) => InfoMap -> LocalExp a -> StateT (Int, Bool) m (LocalExp a)
fuseLocal infos (LFold n key e f) = do
  e' <- fuseRemote infos e
  f' <- fuseRemoteFoldClosure infos f
  return $ LFold n key e' f'

fuseLocal infos (Collect n key e) = do
  e' <- fuseRemote infos e
  return $ Collect n key e'

fuseLocal infos (FromAppl n key e) = do
  e' <- fuseAppl infos e
  return $ FromAppl n key e'

fuseLocal infos (LMap n key f e) = do
  e' <- fuseLocal infos e
  return $ LMap n key f e'

fuseLocal _ e@(LConst _ _ _) = return e


fuseAppl :: (MonadLoggerIO m) => InfoMap -> ApplyExp a -> StateT (Int, Bool) m (ApplyExp a)
fuseAppl infos (Apply f e) = do
  f' <- fuseAppl infos f
  e' <- fuseLocal infos e
  return $ Apply f' e'
fuseAppl _ (ConstApply e) = return (ConstApply e)



