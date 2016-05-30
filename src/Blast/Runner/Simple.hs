{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Runner.Simple
where




import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Map as M

import            Blast.Types
import            Blast.Analyser
import            Blast.Optimizer




runRec :: (Monad m, MonadLoggerIO m) => Config -> JobDesc m a b -> m (a, b)
runRec config@(MkConfig {..}) (jobDesc@MkJobDesc {..}) = do
  $(logInfo) "Analysing"
  (e, count) <- runStateT (expGen seed) 0
  infos <- execStateT (analyseLocal e) M.empty
  (_, e') <-  if shouldOptimize
                then optimize count infos e
                else return (infos, e)
  $(logInfo) "Evaluating"

  (a,b) <- liftIO $ runLocal e'
  a' <- liftIO $ reportingAction a b
  case recPredicate a' of
    True -> do
      $(logInfo) "Finished"
      return (a', b)
    False -> runRec config (jobDesc {seed = a'})



runFun :: ExpClosure a b -> IO (a -> IO b)
runFun (ExpClosure e f) = do
  r <- runLocal e
  return $ f r



runRemote :: RemoteExp a -> IO a
runRemote (RMap _ _ cs e) = do
  f' <- runFun cs
  e' <- runRemote e
  f' e'

runRemote (RConst _ _ e) = return e

runLocal :: LocalExp a -> IO a
runLocal (Collect _ _ e) = runRemote e
runLocal (LConst _ _ a) = return a
runLocal (FMap _ _ f e) = do
  f' <- runLocal f
  e' <- runLocal e
  return $ f' e'

