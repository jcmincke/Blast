{-|
Module      : Blast
Description : A distributed computing library
Copyright   : (c) Jean-Christophe Mincke, 2016
License     : Mozilla Public License, v. 2.0
Maintainer  : jeanchristophe.mincke@gmail.com
Stability   : experimental
Portability : POSIX

-}

module Blast
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
  , lconst
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

import Blast.Types
