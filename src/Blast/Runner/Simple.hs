{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Runner.Simple
where




import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe

import            Blast.Types
import            Blast.Analyser
import            Blast.Optimizer

runRec :: (MonadIO m, MonadLoggerIO m) => Bool -> (a -> StateT Int m (LocalExp (a, b))) -> a -> (a -> Bool) -> m (a, b)
runRec shouldOptimize gen a predicate = do
  $(logInfo) "Analysing"
  (e, count) <- runStateT (gen a) 0
  infos <- execStateT (analyseLocal e) M.empty
  (_, e') <-  if shouldOptimize
                then optimize count infos e
                else return (infos, e)
  $(logInfo) "Evaluating"

  let (a',b) = runLocal e'
  case predicate a' of
    True -> do
      $(logInfo) "Finished"
      return (a', b)
    False -> runRec shouldOptimize gen a' predicate



runFun :: ExpClosure a b -> (a -> b)
runFun (ExpClosure e f) =
  f r
  where
  r = runLocal e



runRemote :: RemoteExp a -> a
runRemote (RMap _ _ e cs) =
  f' e'
  where
  f' = runFun cs
  e' = runRemote e
runRemote (RConst _ _ x) = x

runLocal :: LocalExp a -> a
runLocal (Collect _ _ e) = runRemote e
runLocal (LConst _ _ a) = a
runLocal (FromAppl _ _ e) = runAppl e
runLocal (LMap _ _ e f) =
  f' $ runLocal e
  where
  f' = runFun f

runAppl :: ApplyExp a -> a
runAppl (Apply f e) =
  runAppl f (runLocal e)
runAppl (ConstApply e) = e








