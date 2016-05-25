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

combineClosure :: (MonadLoggerIO m) => ExpClosure a b -> ExpClosure b c -> StateT (Int, Bool) m (ExpClosure a c)
combineClosure (ExpClosure cf f) (ExpClosure cg g)  = do
  (count, _) <- get
  put (count+1, True)
  lc <- liftIO V.newKey
  let cfg = FromAppl count lc (Apply (Apply (ConstApply (,)) cf) cg)
  return $ ExpClosure cfg (\(cf', cg') a -> ((g cg') . (f cf')) a)


fuseClosure :: (MonadLoggerIO m) => InfoMap -> ExpClosure a b -> StateT (Int, Bool) m (ExpClosure a b)
fuseClosure infos (ExpClosure e f) = do
  e' <- fuseLocal infos e
  return $ ExpClosure e' f


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


fuseRemote infos (RMap n key ie g) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ RMap n key ie' g'





fuseRemote _ e@(RConst _ _ _) = return e


fuseLocal :: (MonadLoggerIO m) => InfoMap -> LocalExp a -> StateT (Int, Bool) m (LocalExp a)
fuseLocal infos (Collect n key e) = do
  e' <- fuseRemote infos e
  return $ Collect n key e'

fuseLocal infos (FromAppl n key e) = do
  e' <- fuseAppl infos e
  return $ FromAppl n key e'

fuseLocal infos (LMap n key e f) = do
  e' <- fuseLocal infos e
  return $ LMap n key e' f

fuseLocal _ e@(LConst _ _ _) = return e


fuseAppl :: (MonadLoggerIO m) => InfoMap -> ApplyExp a -> StateT (Int, Bool) m (ApplyExp a)
fuseAppl infos (Apply f e) = do
  f' <- fuseAppl infos f
  e' <- fuseLocal infos e
  return $ Apply f' e'
fuseAppl _ (ConstApply e) = return (ConstApply e)



