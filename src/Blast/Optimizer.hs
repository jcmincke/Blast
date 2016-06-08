{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Optimizer

where

--import Debug.Trace
import qualified  Data.Vault.Strict as V
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Map as M
import qualified  Data.Text as T

import            Blast.Analyser
import            Blast.Syntax ((<$$>), (<**>))
import            Blast.Types



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
  cfg <- (,) <$$> cf <**> cg
  return $ ExpClosure cfg (\(cf', cg') a -> do
    r1 <- f cf' a
    g cg' r1)


fuseClosure :: (MonadLoggerIO m) => InfoMap -> ExpClosure a b -> StateT (Int, Bool) m (ExpClosure a b)
fuseClosure infos (ExpClosure e f) = do
  e' <- fuseLocal infos e
  return $ ExpClosure e' f



fuseRemote :: (MonadLoggerIO m) => InfoMap -> RemoteExp a -> StateT (Int, Bool) m (RemoteExp a)
fuseRemote infos (RMap ne key g ie) | refCountInner > 1 = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ RMap ne key g' ie'   -- inner expression is shared, will be cached
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos (RMap ne key g (RMap _ _ f e)) = do
    f' <- fuseClosure infos f
    g' <- fuseClosure infos g
    fg <- combineClosure f' g'
    fuseRemote infos $ RMap ne key fg e


fuseRemote infos (RMap n key g ie) = do
    g' <- fuseClosure infos g
    ie' <- fuseRemote infos ie
    return $ RMap n key g' ie'

fuseRemote _ e@(RConst _ _ _) = return e


fuseLocal :: (MonadLoggerIO m) => InfoMap -> LocalExp a -> StateT (Int, Bool) m (LocalExp a)
fuseLocal infos (Collect n key e) = do
  e' <- fuseRemote infos e
  return $ Collect n key e'


fuseLocal infos (FMap n key f e) = do
  f' <- fuseLocal infos f
  e' <- fuseLocal infos e
  return $ FMap n key f' e'

fuseLocal _ e@(LConst _ _ _) = return e




