{-
Copyright   : (c) Jean-Christophe Mincke, 2016-2017

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}


module Control.Distributed.Blast.Distributed.Types
(
  CommandClass (..)
  , SlaveRequest (..)
  , SlaveResponse (..)
  , RemoteClosureIndex
  , resetCommand
)
where


import            Control.DeepSeq
import            Data.Binary

import qualified  Data.ByteString as BS
import qualified  Data.Serialize as S

import            GHC.Generics (Generic)

import            Control.Distributed.Blast.Common.Analyser


type RemoteClosureIndex = Int



-- | The list of primitives for master-slave communication.
class (S.Serialize x) => CommandClass s x where
  -- | True if slaves are statefull.
  isStatefullSlave :: s x -> Bool
  -- | The number of slaves.
  getNbSlaves :: s x -> Int

  send :: s x -> Int -> SlaveRequest -> IO (Either String SlaveResponse)

  -- | Stops the system.
  stop :: s x -> IO ()
  setSeed :: s x -> x -> IO (s x)


data SlaveRequest =
  LsReqExecute RemoteClosureIndex
  |LsReqCache Int (Data BS.ByteString)
  |LsReqUncache Int
  |LsReqFetch Int
  |LsReqReset BS.ByteString
  |LsReqBatch Int [SlaveRequest]
  deriving (Generic)

instance Show SlaveRequest where
  show (LsReqExecute n) = "LsReqExecute "++ show n
  show (LsReqCache n _) = "LsReqCache "++ show n
  show (LsReqUncache  n) = "LsReqUncache "++ show n
  show (LsReqFetch  n) = "LsReqFetch "++ show n
  show (LsReqReset _) = "LsReqReset"
  show (LsReqBatch n _) = "LsReqBatch "++ show n


data SlaveResponse =
  LsRespVoid
  |LsRespFetch (Data BS.ByteString)
  |LsRespExecute RemoteClosureResult
  |LsRespBatch (Data BS.ByteString)
  |LsRespError String
  deriving (Generic)

instance Show SlaveResponse where
  show (LsRespError e) = "LsRespError "++e
  show (LsRespVoid) = "LsRespVoid"
  show (LsRespFetch _) = "LsFetch"
  show (LsRespExecute v) = "LocalSlaveExecuteResult "++show v
  show (LsRespBatch _) = "LsRespBatch"

-- | Creates a 'reset' request.
resetCommand  :: BS.ByteString    -- ^ The serialized value of the seed.
              -> SlaveRequest     -- ^ The reset request
resetCommand seedBS = LsReqReset seedBS


instance Binary SlaveRequest
instance Binary SlaveResponse

instance NFData SlaveResponse
instance NFData SlaveRequest



