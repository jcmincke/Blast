
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Distributed.Types
where


import            Control.DeepSeq
import            Data.Binary

import qualified  Data.ByteString as BS
import qualified  Data.Serialize as S

import            GHC.Generics (Generic)

import            Blast.Common.Analyser


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
  LsReqStatus
  |LsReqExecute RemoteClosureIndex
  |LsReqCache Int BS.ByteString
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
  show (LsReqStatus) = "LsReqStatus"
  show (LsReqBatch n _) = "LsReqBatch "++ show n

data SlaveExecuteResult =
  LsExecResCacheMiss Int
  |LsExecResOk
  |LsExecResError String
  deriving (Generic, Show)


data SlaveResponse =
  LsRespBool Bool
  |LsRespError String
  |LsRespVoid
  |LsFetch (Maybe BS.ByteString)
  |LocalSlaveExecuteResult RemoteClosureResult
  |LsRespBatch (Either String BS.ByteString)
  deriving (Generic)

instance Show SlaveResponse where
  show (LsRespBool b) = "LsRespBool "++show b
  show (LsRespError e) = "LsRespError "++e
  show (LsRespVoid) = "LsRespVoid"
  show (LsFetch _) = "LsFetch"
  show (LocalSlaveExecuteResult v) = "LocalSlaveExecuteResult "++show v
  show (LsRespBatch _) = "LsRespBatch"

-- | Creates a 'reset' request.
resetCommand  :: BS.ByteString    -- ^ The serialized value of the seed.
              -> SlaveRequest     -- ^ The reset request
resetCommand seedBS = LsReqReset seedBS

instance Binary SlaveRequest
instance Binary SlaveResponse

instance NFData SlaveResponse
instance NFData SlaveRequest
instance NFData SlaveExecuteResult

