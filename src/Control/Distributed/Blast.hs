{-
Copyright   : (c) Jean-Christophe Mincke, 2016-2017

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}

module Control.Distributed.Blast
(
  -- * Types.
  Computation
  , LocalComputation
  , RemoteComputation
  , Kind (..)
  , Partition
  , Syntax ()

  -- * Classes.

  , Builder
  , Chunkable (..)
  , UnChunkable (..)
  , ChunkableFreeVar (..)

  -- * Core syntax primitives.
  , rapply
  , rconst
  , rconstIO
  , lconst
  , lconstIO
  , collect
  , lapply

  -- * Job description.
  , JobDesc (..)
  , Config (..)
  , defaultConfig

    -- * Helper functions to create closures.
  , fun
  , closure
  , foldFun
  , foldClosure
  , funIO
  , closureIO
  , foldFunIO
  , foldClosureIO



)
where

import Control.Distributed.Blast.Types
