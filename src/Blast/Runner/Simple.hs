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



runClosure :: Fun a b -> (a -> b)
runClosure (Pure f) = f
runClosure (Closure e f) =
  f r
  where
  r = runLocal e


runRemote :: RemoteExp (Rdd a) -> Rdd a
runRemote (Map _ _ e cs) =
  Rdd $ mapMaybe f' rdd
  where
  f' = runClosure cs
  (Rdd rdd) = runRemote e
runRemote (FlatMap _ _ e cs) =
  Rdd $ L.concat $ L.map f' rdd
  where
  f' = runClosure cs
  (Rdd rdd) = runRemote e
runRemote (ConstRemote _ _ (Rdd x)) = Rdd x

runLocal :: LocalExp a -> a
runLocal (Collect _ _ e) = runRemote e
runLocal (ConstLocal _ _ a) = a
runLocal (Fold _ _ e f z) =
  L.foldl' f z rdd
  where
  (Rdd rdd) = runRemote e
runLocal (FromAppl _ _ e) = runAppl e
runLocal (FMap _ _ f e) = f $ runLocal e

runAppl :: ApplExp a -> a
runAppl (Apply' f e) =
  runAppl f (runLocal e)
runAppl (ConstAppl e) = e








