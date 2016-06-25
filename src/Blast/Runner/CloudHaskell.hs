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
import            Control.Monad.Trans.State

import            Control.Distributed.Process hiding (newChan)
import            Control.Distributed.Process.Backend.SimpleLocalnet
import            Control.Distributed.Process.Extras.Internal.Types  (ExitReason(..))
import            Control.Distributed.Process.Extras.Time (Delay(..))
import            Control.Distributed.Process.ManagedProcess as DMP hiding (runProcess, stop)

import            Data.Binary
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe (fromJust)
import qualified  Data.Serialize as S
import            Data.Typeable
import qualified  Data.Vault.Strict as V

import            GHC.Generics (Generic)




import            Blast.Types
import            Blast.Common.Analyser
import            Blast.Master.Analyser as Ma
import            Blast.Distributed.Types
import            Blast.Distributed.Master
import            Blast.Distributed.Slave



data RpcConfig = MkRpcConfig {
  commonConfig :: Config
  , masterConfig :: MasterConfig
  , slaveConfig :: SlaveConfig
  }

data MasterConfig = MkMasterConfig {
  masterLogger :: forall m a. (MonadIO m) => LoggingT m a -> m a
  }

data SlaveConfig = MkSlaveConfig {
  slaveLogger :: forall m a. (MonadIO m) => LoggingT m a -> m a
  }

data RpcRequestControl =
  RpcRequestControlStop

data RpcResponseControl =
  RpcResponseControlError String
  |RpcResponseControlStopped

data SlaveInfo = MkSlaveInfo {
  _slaveIndex :: Int
  , _slaveNodeId :: NodeId
  , _slaveRequestChannel :: TChan (Either RpcRequestControl LocalSlaveRequest)
  , _slaveResponseChannel :: TChan (Either RpcResponseControl LocalSlaveResponse)
  }


data SlaveControl =
  SlaveCtrlStop
  deriving (Typeable, Generic, Show)

instance Binary SlaveControl




slaveProcess :: forall a b . (S.Serialize a, Typeable a, Typeable b) =>
                IO RpcConfig -> JobDesc a b -> Int -> Process ()
slaveProcess configurator (MkJobDesc {..}) slaveIdx = do
  (MkRpcConfig config _ (MkSlaveConfig {..})) <- liftIO configurator
  liftIO $ putStrLn $ "starting slave process: " ++ show slaveIdx
  let slaveState = MkLocalSlave slaveIdx M.empty V.empty expGen config
  let (server::ProcessDefinition (LocalSlave (LoggingT IO) a b)) = defaultProcess {
      apiHandlers = [handleCall (handle slaveLogger)]
    , exitHandlers = [handleExit exitHandler]
    , shutdownHandler = shutdownHandler
    , unhandledMessagePolicy = Drop
    }
  serve slaveState (\ls -> return $ InitOk ls Infinity) server
  where
  handle logger ls req = do
    (resp, ls') <- liftIO $ logger $ runCommand ls req
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
}


rpcCall :: forall a. (S.Serialize a ) => RpcState a -> Int -> LocalSlaveRequest -> IO LocalSlaveResponse
rpcCall (MkRpcState {..}) slaveIdx request = do
    let (MkSlaveInfo {..}) = rpcSlaves M.! slaveIdx
    atomically $ writeTChan _slaveRequestChannel (Right request)

    respE <- atomically $ readTChan _slaveResponseChannel
    case respE of
      Right resp -> return resp
      Left (RpcResponseControlError err) -> return $ LsRespError err
      Left RpcResponseControlStopped -> error "should not reach"



instance (S.Serialize a) => CommandClass RpcState a where
  getNbSlaves (MkRpcState {..}) = M.size rpcSlaves
  status rpc@(MkRpcState {..}) slaveId = do
    (LsRespBool b) <- rpcCall rpc slaveId LsReqStatus
    return b
  exec rpc@(MkRpcState {..}) slaveIdx i = do
    let req = LsReqExecute i
    (LocalSlaveExecuteResult resp) <- rpcCall rpc slaveIdx req
    case resp of
      RemCsResCacheMiss t -> return $ RemCsResCacheMiss t
      ExecRes -> return ExecRes
      ExecResError err -> return (ExecResError err)
  cache rpc@(MkRpcState {..}) slaveIdx i bs = do
    let req = LsReqCache i bs
    (LsRespBool b) <- rpcCall rpc slaveIdx req
    return b

  uncache rpc@(MkRpcState {..}) slaveIdx i = do
    let req = LsReqUncache i
    (LsRespBool b) <- rpcCall rpc slaveIdx req
    return b

  fetch rpc@(MkRpcState {..}) slaveIdx i = do
    let req = LsReqFetch i
    (LsFetch bsM) <- rpcCall rpc slaveIdx req
    case bsM of
      Just bs -> return $ S.decode bs
      Nothing -> return $ Left "Cannot fetch result"

  reset rpc@(MkRpcState {..}) slaveIdx = do
    let seed = fromJust rpcSeed
    let request = LsReqReset (S.encode seed)
    LsRespVoid <- rpcCall rpc slaveIdx request
    return ()

  setSeed rpc@(MkRpcState {..}) a = do
    let rpc' = rpc {rpcSeed = Just a}
    resetAll rpc'
    return rpc'
    where
    resetAll aRpc = do
      let nbSlaves = getNbSlaves aRpc
      let slaveIds = [0 .. nbSlaves - 1]
      _ <- mapConcurrently (\slaveId -> reset aRpc slaveId) slaveIds
      return ()
  -- todo to implement : shutting down slaves
  stop (MkRpcState {..}) = do
    let slaveInfos = M.elems rpcSlaves
    mapM_ (\(MkSlaveInfo {..}) -> do
      atomically $ writeTChan _slaveRequestChannel (Left RpcRequestControlStop)
      _ <- atomically $ readTChan _slaveResponseChannel
      -- todo add error management
      return ()
      ) slaveInfos



startClientRpc :: forall a b. (S.Serialize a, S.Serialize b, CommandClass RpcState a) =>
  RpcConfig
  -> JobDesc a b
  -> (Int -> Closure (Process()))
  -> (a -> b -> IO())
  -> Backend
  -> [NodeId]
  -> Process ()
startClientRpc rpcConfig@(MkRpcConfig _ (MkMasterConfig logger) _) theJobDesc slaveClosure k backend _ = do
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
        let rpcState = MkRpcState slaveInfoMap Nothing
        (a, b) <- liftIO $ do logger $ runComputation rpcConfig 0 rpcState jobDesc
        liftIO $ stop rpcState
        a' <- liftIO $ reportingAction a b
        case recPredicate seed a' b of
          True -> liftIO $ k a b
          False -> do let jobDesc' = jobDesc {seed = a'}
                      liftIO $ putStrLn "iteration finished"
                      loop (n+1) jobDesc'

  runComputation :: (S.Serialize a) =>
    RpcConfig -> Int -> RpcState a -> JobDesc a b -> LoggingT IO (a, b)
  runComputation (MkRpcConfig (MkConfig {..}) _ _)  n rpc (MkJobDesc {..}) = do
    liftIO $ putStrLn ("Start Iteration "++show n)
    let program = expGen seed
    (refMap, _) <- generateReferenceMap 0 M.empty program
--  ((e::MExp 'Local (a,b)), _) <- runStateT (build (expGen seed)) (0::Int)
    (e::MExp 'Local (a,b)) <- build shouldOptimize refMap (0::Int) (1000::Int) program

 --   ((e::MExp 'Local (a,b)), _) <- runStateT (build (expGen seed)) (0::Int)
    infos <- execStateT (Ma.analyseLocal e) M.empty
    rpc' <- liftIO $ setSeed rpc seed
    (r, _) <- evalStateT (runLocal e) (rpc', V.empty, infos)
    return r


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
        (respE::Either ExitReason LocalSlaveResponse) <- safeCall slavePid request
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



runRec :: forall a b. (S.Serialize a, S.Serialize b) =>
  RemoteTable
  -> RpcConfig
  -> [String]
  -> JobDesc a b
  -> (Int -> Closure (Process()))
  -> (a -> b -> IO ())
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

