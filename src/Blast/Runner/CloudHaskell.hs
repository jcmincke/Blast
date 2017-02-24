{-
Copyright   : (c) Jean-Christophe Mincke, 2016-2017

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}



{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}



module Blast.Runner.CloudHaskell
(
  slaveProcess
  , runRec
  , RpcConfig (..)
  , MasterConfig (..)
  , SlaveConfig (..)
)
where

import            Control.Concurrent (threadDelay)
import            Control.Concurrent.Async
import            Control.Concurrent.STM.TChan
import            Control.DeepSeq
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.STM

import            Control.Distributed.Process hiding (newChan, send)
import            Control.Distributed.Process.Backend.SimpleLocalnet
import            Control.Distributed.Process.Extras.Internal.Types  (ExitReason(..))
import            Control.Distributed.Process.Extras.Time (Delay(..))
import            Control.Distributed.Process.ManagedProcess as DMP hiding (runProcess, stop)

import            Data.Binary
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import            Data.Typeable

import            GHC.Generics (Generic)

import            Blast
import            Blast.Distributed.Interface


-- | General configuration.
data RpcConfig = MkRpcConfig {
  commonConfig :: Config          -- ^ Blast configuration.
  , masterConfig :: MasterConfig  -- ^ Specific configuration for master.
  , slaveConfig :: SlaveConfig    -- ^ Specific configuration for slaves.
  }

-- | Master configuration.
data MasterConfig = MkMasterConfig {
  masterLogger :: forall m a. (MonadIO m) => LoggingT m a -> m a  -- ^ Logger.
  }

-- | Master configuration.
data SlaveConfig = MkSlaveConfig {
  slaveLogger :: forall m a. (MonadIO m) => LoggingT m a -> m a   -- ^ Logger.
  }

data RpcRequestControl =
  RpcRequestControlStop

data RpcResponseControl =
  RpcResponseControlError String
  |RpcResponseControlStopped

data SlaveInfo = MkSlaveInfo {
  _slaveIndex :: Int
  , _slaveNodeId :: NodeId
  , _slaveRequestChannel :: TChan (Either RpcRequestControl SlaveRequest)
  , _slaveResponseChannel :: TChan (Either RpcResponseControl SlaveResponse)
  }


data SlaveControl =
  SlaveCtrlStop
  deriving (Typeable, Generic, Show)

instance Binary SlaveControl



-- | Defines the CloudHaskell closure.
slaveProcess :: forall a b . (S.Serialize a, Typeable a, Typeable b) =>
                IO RpcConfig -> JobDesc a b -> Int -> Process ()
slaveProcess configurator jobDesc@(MkJobDesc {..}) slaveIdx = do
  (MkRpcConfig config _ (MkSlaveConfig {..})) <- liftIO configurator
  liftIO $ putStrLn $ "starting slave process: " ++ show slaveIdx
  let slaveContext = makeSlaveContext config slaveIdx jobDesc

  let (server::ProcessDefinition (SlaveContext (LoggingT IO) a b)) = defaultProcess {
      apiHandlers = [handleCall (handle slaveLogger)]
    , exitHandlers = [handleExit exitHandler]
    , shutdownHandler = shutdownHandler
    , unhandledMessagePolicy = Drop
    }
  serve slaveContext (\ls -> return $ InitOk ls Infinity) server
  where
  handle logger ls req = do
    (resp, ls') <- liftIO $ logger $ runCommand req ls
    let resp' = force resp
    replyWith resp' (ProcessContinue ls')
  exitHandler _ _ () = do
    liftIO $ putStrLn "slave exit"
    return $ ProcessStop ExitShutdown
  shutdownHandler _ _ = do
    liftIO $ putStrLn "slave shutdown"
    return $ ()

data RpcState a = MkRpcState {
  rpcSlaves :: M.Map Int SlaveInfo
  -- not sure we should store it there since it is in the job desc
  , rpcSeed :: Maybe a
  , statefullSlaveMode :: Bool
  }


rpcCall :: forall a. (S.Serialize a ) => RpcState a -> Int -> SlaveRequest -> IO SlaveResponse
rpcCall (MkRpcState {..}) slaveIdx request = do
    let (MkSlaveInfo {..}) = rpcSlaves M.! slaveIdx
    atomically $ writeTChan _slaveRequestChannel (Right request)

    respE <- atomically $ readTChan _slaveResponseChannel
    case respE of
      Right resp -> return resp
      Left (RpcResponseControlError err) -> error ("Error in CloudHaskell RPC: " ++ err)
      Left RpcResponseControlStopped -> error "should not reach"


instance (S.Serialize a) => CommandClass RpcState a where
  isStatefullSlave (MkRpcState{ statefullSlaveMode = mode }) = mode
  getNbSlaves (MkRpcState {..}) = M.size rpcSlaves

  send rpc@(MkRpcState {..}) slaveId req = do
    r <- rpcCall rpc slaveId req
    return $ Right r

  stop (MkRpcState {..}) = do
    let slaveInfos = M.elems rpcSlaves
    mapM_ (\(MkSlaveInfo {..}) -> do
      atomically $ writeTChan _slaveRequestChannel (Left RpcRequestControlStop)
      _ <- atomically $ readTChan _slaveResponseChannel
      -- todo add error management
      return ()
      ) slaveInfos

  setSeed rpc@(MkRpcState {..}) a = do
    let rpc' = rpc {rpcSeed = Just a}
    resetAll rpc'
    return rpc'
    where
    resetAll aRpc = do
      let nbSlaves = getNbSlaves aRpc
      let slaveIds = [0 .. nbSlaves - 1]
      let req = resetCommand (S.encode a)
      _ <- mapConcurrently (\slaveId -> send aRpc slaveId req) slaveIds
      return ()


startClientRpc :: forall a b. (S.Serialize a, S.Serialize b, CommandClass RpcState a) =>
  RpcConfig
  -> JobDesc a b
  -> (Int -> Closure (Process()))
  -> (a -> b -> IO ())
  -> Backend
  -> [NodeId]
  -> Process ()
startClientRpc (MkRpcConfig config (MkMasterConfig logger) _) theJobDesc slaveClosure k backend _ = do
  loop 0 theJobDesc
  where
  mkSlaveInfo i nodeId = do
    requestChan <- newTChanIO
    responseChannel <- newTChanIO
    return $ (i, MkSlaveInfo i nodeId requestChan responseChannel)
  findSlaveNodes = do
    selfNode <- getSelfNode
    nodeIds <- liftIO $ findPeers backend 1000000
    return $ nodeIds L.\\ [selfNode]
  loop :: Int -> JobDesc a b -> Process ()
  loop n (jobDesc@MkJobDesc {..}) = do
    nodeIds <- findSlaveNodes
    case nodeIds of
      [] -> do
        liftIO $ putStrLn "No node found, retrying"
        liftIO $ threadDelay 5000000
        loop n jobDesc
      _ -> do
        liftIO $ putStrLn ("Nodes found: " ++ show nodeIds)
        slaveInfos <- liftIO $ mapM (\(i, nodeId) -> mkSlaveInfo i nodeId) $ L.zip [0..] nodeIds
        let slaveInfoMap = M.fromList slaveInfos
        -- create processes that handle RPC
        mapM_ (\slaveInfo -> spawnLocal (startOneClientRpc slaveInfo slaveClosure)) $ M.elems slaveInfoMap
        let rpcState = MkRpcState slaveInfoMap Nothing (statefullSlaves config)
        (a, b) <- liftIO $ do logger $ runComputation config rpcState jobDesc
        liftIO $ stop rpcState
        a' <- liftIO $ reportingAction a b
        case recPredicate seed a' b of
          True -> liftIO $ k a b
          False -> do let jobDesc' = jobDesc {seed = a'}
                      liftIO $ putStrLn "iteration finished"
                      loop (n+1) jobDesc'



startOneClientRpc :: SlaveInfo -> (Int -> Closure (Process ())) -> Process ()
startOneClientRpc (MkSlaveInfo {..}) slaveClosure  = do
  slavePid <- spawn _slaveNodeId (slaveClosure _slaveIndex)
  catchExit
    (localProcess 0 slavePid)
    (\slavePid' () -> do
      liftIO $ putStrLn ("stopping slave from handler:" ++ show slavePid)
      exit slavePid' ()
     -- shutdown slavePid
      )
  where
  localProcess :: Int -> ProcessId -> Process ()
  localProcess nbError slavePid = do
    requestE <- liftIO $ atomically $ readTChan _slaveRequestChannel
    case requestE of
      Right request -> do
        (respE::Either ExitReason SlaveResponse) <- safeCall slavePid request
        case respE of
          Right resp -> do
            liftIO $ atomically $ writeTChan _slaveResponseChannel $ Right resp
            localProcess 0 slavePid
          Left e | nbError < 10 -> do
            liftIO $ putStrLn ("error: "++show e)
            liftIO $ threadDelay 5000000
            -- todo deprecated, fix me
            liftIO $ atomically $ unGetTChan _slaveRequestChannel requestE
            newSlavePid <- spawn _slaveNodeId (slaveClosure _slaveIndex)
            localProcess (nbError+1) newSlavePid
          Left e  -> do
            liftIO $ atomically $ writeTChan _slaveResponseChannel $ Left $ RpcResponseControlError $ show e
            localProcess nbError slavePid
      Left RpcRequestControlStop -> do
              liftIO $ putStrLn ("stopping slave in a controlled way:" ++ show slavePid)
              exit slavePid ()
              liftIO $ atomically $ writeTChan _slaveResponseChannel $ Left $ RpcResponseControlStopped
              liftIO $ putStrLn "Terminating client process"


-- | Run the computation on CloudHaskell
runRec :: forall a b. (S.Serialize a, S.Serialize b) =>
  RemoteTable                         -- ^ CloudHaskell remote table.
  -> RpcConfig                        -- ^ Configuration.
  -> [String]                         -- ^ Command line arguments
  -> JobDesc a b                      -- ^ Job description.
  -> (Int -> Closure (Process()))     -- ^ Function that takes a slave index and returns the CloudHaskell closure to execute on that slave (see "slaveProcess").
  -> (a -> b -> IO ())                -- ^ A continuation that is called when the computation ends.
  -> IO ()
runRec rtable rpcConfig args jobDesc slaveClosure k = do
  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port rtable
      startMaster backend (startClientRpc rpcConfig jobDesc slaveClosure k backend)
      putStrLn ("End")
    ["slave", host, port] -> do
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> putStrLn ("Bad args: " ++ show args)

