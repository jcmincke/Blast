
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
  execute :: (S.Serialize c, S.Serialize a, S.Serialize b) => s x -> Int -> RemoteClosureIndex -> (RemoteValue c) -> (RemoteValue a) -> ResultDescriptor b -> IO (RemoteClosureResult b)
  cache :: s x -> Int -> Int -> BS.ByteString -> IO Bool
  cache' :: (S.Serialize a) => s x -> Int -> Int -> a -> IO Bool
  uncache :: s x -> Int -> Int -> IO Bool
  isCached :: s x -> Int -> Int -> IO Bool
  reset :: s x -> Int -> IO ()
  setSeed :: s x -> x -> IO (s x)
  stop :: s x -> IO ()



data LocalSlaveRequest =
  LsReqStatus
  |LsReqExecute RemoteClosureIndex (RemoteValue BS.ByteString) (RemoteValue BS.ByteString) (ResultDescriptor BS.ByteString)
  |LsReqCache Int BS.ByteString
  |LsReqUncache Int
  |LsReqIsCached Int
  |LsReqReset BS.ByteString
  deriving (Generic)

instance Show LocalSlaveRequest where
  show (LsReqExecute _ _ _ _) = "LsReqExecute"
  show (LsReqCache _ _) = "LsReqCache"
  show (LsReqUncache  _) = "LsReqUncache"
  show (LsReqIsCached  _) = "LsReqIsCached"
  show (LsReqReset _) = "LsReqReset"
  show (LsReqStatus) = "LsReqStatus"

data LocalSlaveExecuteResult =
  LsExecResCacheMiss Int
  |LsExecRes (Maybe BS.ByteString)
  |LsExecResError String
  deriving (Generic, Show)


data LocalSlaveResponse =
  LsRespBool Bool
  |LsRespError String
  |LsRespVoid
  |LocalSlaveExecuteResult (RemoteClosureResult BS.ByteString)
  deriving (Generic)

instance Show LocalSlaveResponse where
  show (LsRespBool _) = "LsRespBool"
  show (LsRespError _) = "LsRespError"
  show (LsRespVoid) = "LsRespVoid"
  show (LocalSlaveExecuteResult _) = "LocalSlaveExecuteResult"



instance Binary LocalSlaveRequest
instance Binary LocalSlaveResponse

instance NFData LocalSlaveResponse
instance NFData LocalSlaveRequest
instance NFData LocalSlaveExecuteResult

