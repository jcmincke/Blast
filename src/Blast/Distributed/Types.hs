
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

import Blast.Internal.Types


type RemoteClosureIndex = Int

class (S.Serialize x) => RemoteClass s x where
  getNbSlaves :: s x -> Int
  status ::  s x -> Int -> IO Bool
  execute :: s x -> Int -> RemoteClosureIndex -> IO RemoteClosureResult
  cache :: s x -> Int -> Int -> BS.ByteString -> IO Bool
  uncache :: s x -> Int -> Int -> IO Bool
  fetch :: (S.Serialize a) => s x -> Int -> Int -> IO (Either String a)
  reset :: s x -> Int -> IO ()
  setSeed :: s x -> x -> IO (s x)
  stop :: s x -> IO ()



data LocalSlaveRequest =
  LsReqStatus
  |LsReqExecute RemoteClosureIndex
  |LsReqCache Int BS.ByteString
  |LsReqUncache Int
  |LsReqFetch Int
  |LsReqReset BS.ByteString
  deriving (Generic)

instance Show LocalSlaveRequest where
  show (LsReqExecute _) = "LsReqExecute"
  show (LsReqCache _ _) = "LsReqCache"
  show (LsReqUncache  _) = "LsReqUncache"
  show (LsReqFetch  _) = "LsReqFetch"
  show (LsReqReset _) = "LsReqReset"
  show (LsReqStatus) = "LsReqStatus"

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
  deriving (Generic)

instance Show LocalSlaveResponse where
  show (LsRespBool _) = "LsRespBool"
  show (LsRespError _) = "LsRespError"
  show (LsRespVoid) = "LsRespVoid"
  show (LsFetch _) = "LsFetch"
  show (LocalSlaveExecuteResult _) = "LocalSlaveExecuteResult"



instance Binary LocalSlaveRequest
instance Binary LocalSlaveResponse

instance NFData LocalSlaveResponse
instance NFData LocalSlaveRequest
instance NFData LocalSlaveExecuteResult

