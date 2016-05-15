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
  let (a',b) = runLocal e'
  case predicate a' of
    True -> return (a', b)
    False -> runRec shouldOptimize gen a' predicate



runFun :: Fun a b -> (a -> b)
runFun (Pure f) = f
runFun (Closure e f) =
  f r
  where
  r = runLocal e


runFoldFun :: FoldFun a r -> (r -> a -> r)
runFoldFun (FoldPure f) = f
runFoldFun (FoldClosure e f) =
  f r
  where
  r = runLocal e


runRemoteFoldClosure :: PreparedFoldClosure a r -> (r, (r -> a -> r))
runRemoteFoldClosure (PreparedFoldClosure e f) =
  (z, f c)
  where
  (c, z) = runLocal e



runRemote :: RemoteExp (Rdd a) -> Rdd a
runRemote (RMap _ _ e cs) =
  Rdd $ mapMaybe f' rdd
  where
  f' = runFun cs
  (Rdd rdd) = runRemote e
runRemote (RFold _ _ e cs) =
  Rdd $ [L.foldl' f' z rdd]
  where
  (z, f') = runRemoteFoldClosure cs
  (Rdd rdd) = runRemote e
runRemote (RFlatMap _ _ e cs) =
  Rdd $ L.concat $ L.map f' rdd
  where
  f' = runFun cs
  (Rdd rdd) = runRemote e
runRemote (RConst _ _ (Rdd x)) = Rdd x

runLocal :: LocalExp a -> a
runLocal (Collect _ _ e) = runRemote e
runLocal (LConst _ _ a) = a
runLocal (LFold _ _ e cs) =
  L.foldl' f' z rdd
  where
  (z, f') = runRemoteFoldClosure cs
  (Rdd rdd) = runRemote e
runLocal (FromAppl _ _ e) = runAppl e
runLocal (LMap _ _ f e) =
  f' $ runLocal e
  where
  f' = runFun f

runAppl :: ApplyExp a -> a
runAppl (Apply f e) =
  runAppl f (runLocal e)
runAppl (ConstApply e) = e








