
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

class (S.Serialize x) => CommandClass s x where
  isStatefullSlave :: s x -> Bool
  getNbSlaves :: s x -> Int
  status ::  s x -> Int -> IO Bool
  exec :: s x -> Int -> RemoteClosureIndex -> IO RemoteClosureResult
  cache :: s x -> Int -> Int -> BS.ByteString -> IO Bool
  uncache :: s x -> Int -> Int -> IO Bool
  fetch :: (S.Serialize a) => s x -> Int -> Int -> IO (Either String a)
  reset :: s x -> Int -> IO ()
  setSeed :: s x -> x -> IO (s x)
  stop :: s x -> IO ()
  batch :: (S.Serialize a) => s x -> Int -> Int -> [LocalSlaveRequest] -> IO (Either String a)


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

