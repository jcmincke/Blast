
{-# LANGUAGE BangPatterns #-}
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



module Blast.Distributed.Rpc.CloudHaskell

where

import            Control.Concurrent
import            Control.Concurrent.Async
import            Control.DeepSeq
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import            Control.Distributed.Process hiding (newChan)
import            Control.Distributed.Process.Backend.SimpleLocalnet
import            Control.Distributed.Process.Extras.Internal.Types  (ExitReason(..))
import            Control.Distributed.Process.Extras.Time (Delay(..))
import            Control.Distributed.Process.ManagedProcess as DMP hiding (runProcess, stop)

import            Data.Binary
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe (fromJust)
import qualified  Data.Serialize as S
import            Data.Typeable
import qualified  Data.Vault.Strict as V

import            GHC.Generics (Generic)




import            Blast.Types
import            Blast.Analyser
import            Blast.Optimizer
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
  , _slaveRequestChannel :: Chan (Either RpcRequestControl LocalSlaveRequest)
  , _slaveResponseChannel :: Chan (Either RpcResponseControl LocalSlaveResponse)
  }


data SlaveControl =
  SlaveCtrlStop
  deriving (Typeable, Generic, Show)

instance Binary SlaveControl




slaveProcess :: forall a b . (S.Serialize a, Typeable a, Typeable b) =>
                IO RpcConfig -> JobDesc (LoggingT IO) a b -> Int -> Process ()
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
    liftIO $ putStrLn "slaveProcess: received cmd"
    liftIO $ print req
    (resp, ls') <- liftIO $ logger $ runCommand ls req
    let resp' = force resp
    liftIO $ putStrLn "slaveProcess: processed cmd"
    liftIO $ print resp'
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
    putStrLn "rpcCAll: running process "
    writeChan _slaveRequestChannel (Right request)

    respE <- readChan _slaveResponseChannel
    putStrLn "rpcCAll: got for resp from chan"
    case respE of
      Right resp -> return resp
      Left (RpcResponseControlError err) -> return $ LsRespError err



instance (S.Serialize a) => RemoteClass RpcState a where
  getNbSlaves (MkRpcState {..}) = M.size rpcSlaves
  status rpc@(MkRpcState {..}) slaveId = do
    (LsRespBool b) <- rpcCall rpc slaveId LsReqStatus
    return b
  execute rpc@(MkRpcState {..}) slaveIdx i = do
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
      liftIO $ putStrLn "setSeed: before "
      _ <- mapConcurrently (\slaveId -> reset aRpc slaveId) slaveIds
      liftIO $ putStrLn "setSeed: after "
      return ()
  -- todo to implement : shutting down slaves
  stop (MkRpcState {..}) = do
    let slaveInfos = M.elems rpcSlaves
    mapM_ (\(MkSlaveInfo {..}) -> do
      writeChan _slaveRequestChannel (Left RpcRequestControlStop)
      _ <- readChan _slaveResponseChannel
      -- todo add error management
      return ()
      ) slaveInfos



startClientRpc :: forall a b. (S.Serialize a, S.Serialize b, RemoteClass RpcState a) => RpcConfig -> JobDesc (LoggingT IO) a b ->
   (Int -> Closure (Process())) -> (a -> b -> IO()) -> Backend -> [NodeId] -> Process ()
startClientRpc rpcConfig@(MkRpcConfig _ (MkMasterConfig logger) _) theJobDesc slaveClosure k backend _ = do
  loop 0 theJobDesc
  where
  mkSlaveInfo i nodeId = do
    requestChan <- newChan
    responseChannel <- newChan
    return $ (i, MkSlaveInfo i nodeId requestChan responseChannel)
  findSlaveNodes = do
    selfNode <- getSelfNode
    nodeIds <- liftIO $ findPeers backend 1000000
    return $ nodeIds L.\\ [selfNode]
  loop :: Int -> JobDesc (LoggingT IO) a b -> Process ()
  loop n (jobDesc@MkJobDesc {..}) = do
    nodeIds <- findSlaveNodes
    case nodeIds of
      [] -> do
        liftIO $ putStrLn "No node found, retrying"
        liftIO $ threadDelay 5000000
        loop n jobDesc
      _ -> do
        liftIO $ putStrLn ("Nodes found: " ++ show nodeIds)
      --  liftIO $ getLine
        slaveInfos <- liftIO $ mapM (\(i, nodeId) -> mkSlaveInfo i nodeId) $ L.zip [0..] nodeIds
        let slaveInfoMap = M.fromList slaveInfos
        -- create processes that handle RPC
        mapM_ (\slaveInfo -> spawnLocal (startOneClientRpc slaveInfo slaveClosure)) $ M.elems slaveInfoMap
        let rpcState = MkRpcState slaveInfoMap Nothing
        (a, b) <- liftIO $ do logger $ runComputation rpcConfig 0 rpcState jobDesc
        liftIO $ stop rpcState
        a' <- liftIO $ reportingAction a b
        case recPredicate a of
          True -> liftIO $ k a b
          False -> do let jobDesc' = jobDesc {seed = a'}
                      liftIO $ putStrLn "iteration finished"
                      --liftIO $ getLine
                      loop (n+1) jobDesc'

  runComputation :: RpcConfig -> Int -> RpcState a -> JobDesc (LoggingT IO) a b -> LoggingT IO (a, b)
  runComputation (MkRpcConfig (MkConfig {..}) (MkMasterConfig logger) _)  n rpc (MkJobDesc {..}) = do
    liftIO $ putStrLn ("Start Iteration "++show n)
    (e, count) <- runStateT (expGen seed) 0
    infos <- execStateT (analyseLocal e) M.empty
    (infos', e') <- if shouldOptimize
                      then logger $ optimize count infos e
                      else return (infos, e)
    rpc' <- liftIO $ setSeed rpc seed
    (r, _) <- evalStateT (runSimpleLocal infos' e') (rpc', V.empty)
    return r



startOneClientRpc :: SlaveInfo -> (Int -> Closure (Process ())) -> Process ()
startOneClientRpc (MkSlaveInfo {..}) slaveClosure  = do
  slavePid <- spawn _slaveNodeId (slaveClosure _slaveIndex)
  liftIO $ print ("slave pid ", slavePid)
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
    requestE <- liftIO $ readChan _slaveRequestChannel
    case requestE of
      Right request -> do
        liftIO $ putStrLn ("mkRpcProcess: before call " ++ show _slaveIndex )
        (respE::Either ExitReason LocalSlaveResponse) <- safeCall slavePid request
        liftIO $ putStrLn ("mkRpcProcess: after call " ++ show _slaveIndex )
        case respE of
          Right resp -> do
            liftIO $ writeChan _slaveResponseChannel $ Right resp
            localProcess 0 slavePid
          Left e | nbError < 10 -> do
            liftIO $ putStrLn ("error: "++show e)
            liftIO $ threadDelay 5000000
            -- todo deprecated, fix me
            liftIO $ unGetChan _slaveRequestChannel requestE
            newSlavePid <- spawn _slaveNodeId (slaveClosure _slaveIndex)
            localProcess (nbError+1) newSlavePid
          Left e  -> do
            liftIO $ writeChan _slaveResponseChannel $ Left $ RpcResponseControlError $ show e
            localProcess nbError slavePid
      Left RpcRequestControlStop -> do
              liftIO $ putStrLn ("stopping slave in a controlled way:" ++ show slavePid)
              exit slavePid ()
              liftIO $ writeChan _slaveResponseChannel $ Left $ RpcResponseControlStopped
              liftIO $ putStrLn "Terminating client process"



runRpc :: forall a b. (S.Serialize a, S.Serialize b, RemoteClass RpcState a) =>
  RemoteTable
  -> RpcConfig
  -> [String]
  -> JobDesc (LoggingT IO) a b
  -> (Int -> Closure (Process()))
  -> (a -> b -> IO ())
  -> IO()
runRpc rtable rpcConfig args jobDesc slaveClosure k = do
  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port rtable
      startMaster backend (startClientRpc rpcConfig jobDesc slaveClosure k backend)
      putStrLn ("End")
    ["slave", host, port] -> do
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> putStrLn ("Bad args: " ++ show args)

