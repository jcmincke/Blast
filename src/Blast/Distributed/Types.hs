
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
  -- | Status of a slave.
  status  :: s x
          -> Int        -- ^ Slave index
          -> IO Bool
  -- | Remotely executes a closure.
  exec  :: s x
        -> Int                      -- ^ Slave index.
        -> RemoteClosureIndex       -- ^ Index of the remote closure
        -> IO RemoteClosureResult   -- ^ Result
  -- | Remotely caches a serialized value.
  cache :: s x
        -> Int            -- ^ Slave index.
        -> Int            -- ^ Node index.
        -> BS.ByteString  -- ^ Serialized value to cache.
        -> IO Bool
  -- | Remotely uncaches value associated with a node.
  uncache :: s x
          -> Int      -- ^ Slave index.
          -> Int      -- ^ Node index.
          -> IO Bool
  -- | Fetch the remote value associated with a node.
  fetch :: (S.Serialize a)
        => s x
        -> Int                  -- ^ Slave index.
        -> Int                  -- ^ Node index.
        -> IO (Either String a)
  -- | Reset the given slave.
  reset :: s x
        -> Int    -- ^ Slave index.
        -> IO ()
  -- | Set the computation generator seed.
  setSeed :: s x -> x -> IO (s x)
  -- | Stops the system.
  stop :: s x -> IO ()

  -- | Sends a list of requests as a batch.
  -- Returns the value associated with the given node.
  -- Uses when slaves are stateless.
  batch :: (S.Serialize a)
        => s x
        -> Int                    -- ^ Slave index.
        -> Int                    -- ^ Node index.
        -> [LocalSlaveRequest]    -- ^ List of requests.
        -> IO (Either String a)   -- ^ Value associated with the specified node.


data LocalSlaveRequest =
  LsReqStatus
  |LsReqExecute RemoteClosureIndex
  |LsReqCache Int BS.ByteString
  |LsReqUncache Int
  |LsReqFetch Int
  |LsReqReset BS.ByteString
  |LsReqBatch Int [LocalSlaveRequest]
  deriving (Generic)

instance Show LocalSlaveRequest where
  show (LsReqExecute n) = "LsReqExecute "++ show n
  show (LsReqCache n _) = "LsReqCache "++ show n
  show (LsReqUncache  n) = "LsReqUncache "++ show n
  show (LsReqFetch  n) = "LsReqFetch "++ show n
  show (LsReqReset _) = "LsReqReset"
  show (LsReqStatus) = "LsReqStatus"
  show (LsReqBatch n _) = "LsReqBatch "++ show n

data LocalSlaveExecuteResult =
  LsExecResCacheMiss Int
  |LsExecResOk
  |LsExecResError String
  deriving (Generic, Show)


data LocalSlaveResponse =
  LsRespBool Bool
  |LsRespError String
  |LsRespVoid
  |LsFetch (Maybe BS.ByteString)
  |LocalSlaveExecuteResult RemoteClosureResult
  |LsRespBatch (Either String BS.ByteString)
  deriving (Generic)

instance Show LocalSlaveResponse where
  show (LsRespBool b) = "LsRespBool "++show b
  show (LsRespError e) = "LsRespError "++e
  show (LsRespVoid) = "LsRespVoid"
  show (LsFetch _) = "LsFetch"
  show (LocalSlaveExecuteResult v) = "LocalSlaveExecuteResult "++show v
  show (LsRespBatch _) = "LsRespBatch"



instance Binary LocalSlaveRequest
instance Binary LocalSlaveResponse

instance NFData LocalSlaveResponse
instance NFData LocalSlaveRequest
instance NFData LocalSlaveExecuteResult

