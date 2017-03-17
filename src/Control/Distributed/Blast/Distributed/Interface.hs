{-
Copyright   : (c) Jean-Christophe Mincke, 2016-2017

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}



{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}

module Control.Distributed.Blast.Distributed.Interface
(
  -- * Types
  SlaveContext
  , SlaveRequest
  , SlaveResponse

  -- * Class
  , CommandClass (..)

  -- * Functions

  , runCommand

  , resetCommand

  , runComputation
  , makeSlaveContext
)
where


--import Debug.Trace
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V


import            Control.Distributed.Blast.Distributed.Master (runLocal)
import            Control.Distributed.Blast.Distributed.Slave
import            Control.Distributed.Blast.Distributed.Types (resetCommand, CommandClass (..), SlaveRequest(..), SlaveResponse(..))
import            Control.Distributed.Blast.Master.Analyser (analyseLocal)
import            Control.Distributed.Blast.Types

-- | Executes a computation on the master node using the given instance of the "CommandClass" to delegate work to the slaves.
runComputation :: (S.Serialize a, CommandClass s a, MonadLoggerIO m)
  => Config               -- ^ Configuration.
  -> s a                  -- ^ Instance of a "CommandClass".
  -> JobDesc a b          -- ^ Job description.
  -> m (a, b)             -- ^ Result : new value of the seed and the computation output.
runComputation (MkConfig {..}) s (MkJobDesc {..}) = do
    let program = computationGen seed

    (refMap, count) <- generateReferenceMap 0 M.empty program
    e <- build refMap count program

    infos <- execStateT (analyseLocal e) M.empty
    s' <- liftIO $ setSeed s seed
    (r, _) <- evalStateT (runLocal e) (s', V.empty, infos)

    return r






