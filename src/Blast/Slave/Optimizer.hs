{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Slave.Optimizer

where

--import Debug.Trace
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Map as M
import qualified  Data.Text as T

import            Blast.Common.Analyser
import            Blast.Slave.Analyser
import            Blast.Syntax ((<$$>), (<**>))
import            Blast.Types

nextIndex :: (Monad m) => StateT (Int, Bool) m ()
nextIndex = do
  (index, a) <- get
  put (index+1, a)


optimize :: (MonadLoggerIO m) => Int -> InfoMap -> SExp 'Local a -> m (InfoMap, SExp 'Local a)
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


--combineClosure :: (MonadLoggerIO m) => ExpClosure SExp a b -> ExpClosure SExp b c -> StateT (Int, Bool) m (ExpClosure SExp a c)
combineClosure :: forall a b c  m. (MonadIO m) => ExpClosure SExp a b -> ExpClosure SExp b c -> StateT (Int, Bool) m (ExpClosure SExp a c)
combineClosure (ExpClosure cf f) (ExpClosure cg g)  = do
  cfg <- combineFreeVars cf cg
  return $ ExpClosure cfg (\(cf', cg') a -> do
    r1 <- f cf' a
    g cg' r1)

combineFreeVars :: (MonadIO m) => SExp 'Local a -> SExp 'Local b -> StateT (Int, Bool) m (SExp 'Local (a, b))
combineFreeVars cf cg = do
    (index, _) <- get
    p <- liftIO $ runStateT (build $ ((,) <$$> cf <**> cg)) index
    let (cfg, index') = p
    put (index', True)
    return cfg



fuseClosure :: (MonadLoggerIO m) => InfoMap -> ExpClosure SExp a b -> StateT (Int, Bool) m (ExpClosure SExp a b)
fuseClosure infos (ExpClosure e f) = do
  e' <- fuseLocal infos e
  return $ ExpClosure e' f



fuseRemote :: (MonadLoggerIO m) => InfoMap -> SExp 'Remote a -> StateT (Int, Bool) m (SExp 'Remote a)
fuseRemote infos (SRApply ne key g ie) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ SRApply ne key g' ie'   -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (SRApply ne key g (SRApply _ _ f e)) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    fuseRemote infos $ SRApply ne key fg e


fuseRemote infos (SRApply n key g ie) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ SRApply n key g' ie'

fuseRemote _ e@(SRConst _ _ _) = return e


fuseLocal :: (MonadLoggerIO m) => InfoMap -> SExp 'Local a -> StateT (Int, Bool) m (SExp 'Local a)
fuseLocal infos (SCollect n key e) = do
  e' <- fuseRemote infos e
  return $ SCollect n key e'


fuseLocal infos (SLApply n key f e) = do
  f' <- fuseLocal infos f
  e' <- fuseLocal infos e
  return $ SLApply n key f' e'

fuseLocal _ e@(SLConst _ _ _) = return e




