module Blast
(
  Computation
  , LocalComputation
  , RemoteComputation
  , Kind (..)
  , Partition
  , Chunkable (..)
  , UnChunkable (..)
  , ChunkableFreeVar (..)
  -- , Fun ()
  -- , FoldFun ()
  , Syntax ()
  , rapply
  , rconst
  , lconst
  , collect
  , lapply
  , JobDesc (..)
  , Config (..)
  , defaultConfig

)
where

import Blast.Types
