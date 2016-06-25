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
{-}
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
-}



{-}

fuseLocal :: (MonadLoggerIO m) => InfoMap -> MExp 'Local a -> StateT (Int, Bool) m (MExp 'Local a)
fuseLocal infos (MCollect n key e) = do
  e' <- fuseRemote infos e
  return $ MCollect n key e'


fuseLocal infos (MLApply n key f e) = do
  f' <- fuseLocal infos f
  e' <- fuseLocal infos e
  return $ MLApply n key f' e'

fuseLocal _ e@(MLConst _ _ _) = return e

-}


