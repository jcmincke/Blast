{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Master.Optimizer

where

--import Debug.Trace
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Map as M
import qualified  Data.Text as T

import            Blast.Common.Analyser
import            Blast.Master.Analyser
import            Blast.Syntax ((<$$>), (<**>))
import            Blast.Types

nextIndex :: (Monad m) => StateT (Int, Bool) m ()
nextIndex = do
  (index, a) <- get
  put (index+1, a)


optimize :: (MonadLoggerIO m) => Int -> MExp 'Local a -> m (InfoMap, MExp 'Local a)
optimize = do
    go (1::Int)
    where
    go nbIter count e = do
      $(logInfo) $ T.pack ("Start Optimization: iteration nb " ++ show nbIter)
      infos <- execStateT (analyseLocal e) M.empty
      (e', (count', b)) <- runStateT (fuseLocal infos e) (count, False)
      if b
        then do go (nbIter+1) count' e'
        else do infos' <- execStateT (analyseLocal e') M.empty
                $(logInfo) $ T.pack ("Exiting optimization after "++ show nbIter ++ " iteration(s)")
                return (infos', e')


optimized :: (MonadLoggerIO m) => StateT (Int, Bool) m ()
optimized = do
  (count, _) <- get
  put (count, True)


combineClosure :: forall a b c  m. (MonadIO m) => ExpClosure MExp a b -> ExpClosure MExp b c -> StateT (Int, Bool) m (ExpClosure MExp a c)
combineClosure (ExpClosure cf f) (ExpClosure cg g)  = do
  cfg <- combineFreeVars cf cg
  return $ ExpClosure cfg (\(cf', cg') a -> do
    r1 <- f cf' a
    g cg' r1)

combineFreeVars :: (MonadIO m) => MExp 'Local a -> MExp 'Local b -> StateT (Int, Bool) m (MExp 'Local (a, b))
combineFreeVars cf cg = do
    (index, _) <- get
    p <- liftIO $ runStateT (build $ ((,) <$$> cf <**> cg)) index
    let (cfg, index') = p
    put (index', True)
    return cfg



fuseClosure :: (MonadLoggerIO m) => InfoMap -> ExpClosure MExp a b -> StateT (Int, Bool) m (ExpClosure MExp a b)
fuseClosure infos (ExpClosure e f) = do
  e' <- fuseLocal infos e
  return $ ExpClosure e' f



fuseRemote :: (MonadLoggerIO m) => InfoMap -> MExp 'Remote a -> StateT (Int, Bool) m (MExp 'Remote a)
fuseRemote infos (MRApply ne g ie) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ MRApply ne g' ie'   -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (MRApply ne g (MRApply ni f e)) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    e' <- fuseRemote infos e
    fuseRemote infos $ MRApply ne fg e'


fuseRemote infos (MRApply n g ie) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ MRApply n g' ie'

fuseRemote _ e@(MRConst _ _ _) = return e


fuseLocal :: (MonadLoggerIO m) => InfoMap -> MExp 'Local a -> StateT (Int, Bool) m (MExp 'Local a)
fuseLocal infos (MCollect n key e) = do
  e' <- fuseRemote infos e
  return $ MCollect n key e'


fuseLocal infos (MLApply n key f e) = do
  f' <- fuseLocal infos f
  e' <- fuseLocal infos e
  return $ MLApply n key f' e'

fuseLocal _ e@(MLConst _ _ _) = return e




