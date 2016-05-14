
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE FlexibleContexts #-}



module Blast.Distributed.Rpc.CloudHaskell

where

import            Control.Concurrent
import            Control.Concurrent.Async
import            Control.DeepSeq
import            Control.Monad
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import            Data.Binary
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe (fromJust)
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import            Data.Typeable
import qualified  Data.Vault.Strict as V

import            GHC.Generics (Generic)

import            System.Environment (getArgs)
import            Control.Distributed.Process hiding (newChan)
import            Control.Distributed.Process.Node (initRemoteTable, LocalNode, runProcess)
import            Control.Distributed.Process.Backend.SimpleLocalnet
import            Control.Distributed.Process.Closure (mkClosure, remotable, mkStatic)
import            Control.Distributed.Process.Extras.Internal.Types  (ExitReason(..))
import            Control.Distributed.Process.Extras.Time (Delay(..))
import            Control.Distributed.Process.ManagedProcess as DMP hiding (runProcess)
import            Control.Distributed.Process.Serializable



import            Blast.Types
import            Blast.Analyser
import            Blast.Syntax
import            Blast.Optimizer
import            Blast.Distributed.Types
import            Blast.Distributed.Master
import            Blast.Distributed.Slave



data JobDesc a b = MkJobDesc {
  shouldOptimize :: Bool
  , seed :: a
  , expGen :: a -> StateT Int (LoggingT IO) (LocalExp (a, b))
  , recPredicate :: a -> Bool
  }

data SlaveInfo = MkSlaveInfo {
  _slaveIndex :: Int
  , _slaveProcessId :: ProcessId
  }


data SlaveControl =
  SlaveCtrlStop
  deriving (Typeable, Generic, Show)

instance Binary SlaveControl




slaveProcess :: forall a b . (S.Serialize a, Typeable a, Typeable b) =>
                JobDesc a b -> Int -> Process ()
slaveProcess (MkJobDesc {..}) slaveIdx = do
  liftIO $ putStrLn $ "starting slave process: " ++ show slaveIdx
  let slaveState = MkLocalSlave slaveIdx M.empty V.empty expGen

  let (server::ProcessDefinition (LocalSlave (LoggingT IO) a b)) = defaultProcess {
      apiHandlers = [handleCall handle]
    , unhandledMessagePolicy = Drop
    }
  serve slaveState (\ls -> return $ InitOk ls Infinity) server
  where
  handle ls req = do
    (resp, ls') <- liftIO $ runStdoutLoggingT $ runCommand ls req
    let resp' = force resp
    replyWith resp' (ProcessContinue ls')



-- process that makes a call to a slave
mkRpcProcess :: forall a. (S.Serialize a) =>
  a -> LocalSlaveRequest -> Chan (Either String LocalSlaveResponse) -> SlaveInfo -> Process()
mkRpcProcess seed req respChan (MkSlaveInfo {..}) = do
  liftIO $ print _slaveProcessId
  (respE::Either ExitReason LocalSlaveResponse) <- safeCall _slaveProcessId ( req)
  case respE of
    Right resp -> do
      liftIO $ writeChan respChan $ Right resp
    Left e -> liftIO $ writeChan respChan $ Left $ show e

data RpcState a = MkRpcState {
  rpcSlaves :: M.Map Int SlaveInfo
  , rpcSeed :: Maybe a
  , rpcLocalNode :: LocalNode
}




rpcCall :: forall a. (S.Serialize a ) => RpcState a -> Int -> LocalSlaveRequest -> IO LocalSlaveResponse
rpcCall (MkRpcState {..}) slaveIdx req = do
    let slaveInfo = rpcSlaves M.! slaveIdx
    respChan <- newChan
    let seed = fromJust rpcSeed
    runProcess rpcLocalNode (mkRpcProcess seed req respChan slaveInfo)
    respE <- readChan respChan
    case respE of
      Right resp -> return resp
      Left err -> return $ LsRespError err



encodeRemoteValue :: (S.Serialize a) => RemoteValue a -> RemoteValue BS.ByteString
encodeRemoteValue (RemoteValue a) = RemoteValue $ S.encode a
encodeRemoteValue CachedRemoteValue = CachedRemoteValue


instance (S.Serialize a) => RemoteClass RpcState a where
  getNbSlaves (MkRpcState {..}) = M.size rpcSlaves
  status rpc@(MkRpcState {..}) slaveId = do
    (LsRespBool b) <- rpcCall rpc slaveId LsReqStatus
    return b
  execute rpc@(MkRpcState {..}) slaveIdx i c a (ResultDescriptor sr sc) = do
    let ec = encodeRemoteValue c
    let ea = encodeRemoteValue a
    let req = LsReqExecute i ec ea (ResultDescriptor sr sc)
    (LocalSlaveExecuteResult resp) <- rpcCall rpc slaveIdx req
    case resp of
      RemCsResCacheMiss t -> return $ RemCsResCacheMiss t
      ExecRes Nothing -> return (ExecRes Nothing)
      ExecRes (Just bs) -> do
        case S.decode bs of
          Left err -> error ("decode failed: " ++ err)
          Right r -> return (ExecRes $ Just r)
      ExecResError err -> return (ExecResError err)
  cache rpc@(MkRpcState {..}) slaveIdx i a = do
    let bs = S.encode a
    let req = LsReqCache i bs
    (LsRespBool b) <- rpcCall rpc slaveIdx req
    return b

  uncache rpc@(MkRpcState {..}) slaveIdx i = do
    let req = LsReqUncache i
    (LsRespBool b) <- rpcCall rpc slaveIdx req
    return b

  isCached rpc@(MkRpcState {..}) slaveIdx i = do
    let req = LsReqIsCached i
    (LsRespBool b) <- rpcCall rpc slaveIdx req
    return b


  reset rpc@(MkRpcState {..}) slaveIdx = do
    let (MkSlaveInfo {..}) = rpcSlaves M.! slaveIdx
    respChan <- newChan
    let seed = fromJust rpcSeed
    let req = LsReqReset True (S.encode seed)
    let resetProcess = do
          (respE::Either ExitReason LocalSlaveResponse) <- safeCall _slaveProcessId req
          case respE of
            Right resp -> do
              liftIO $ writeChan respChan $ Right resp
            Left e -> liftIO $ writeChan respChan $ Left $ show e
    runProcess rpcLocalNode resetProcess
    respE <- readChan respChan
    case respE of
      Right LsRespVoid -> return ()
      Right _ -> error "Error in reset"
      Left err -> error err

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
  stop _ = return ()

startClientRpc :: forall a b. (S.Serialize a, S.Serialize b, RemoteClass RpcState a) => JobDesc a b ->
   (Int -> Closure (Process())) -> (a -> b -> IO()) -> Backend -> [NodeId] -> Process ()
startClientRpc jobDesc slaveClosure k backend nodeIds = do
  slavePids <- mapM (\(nodeId, slaveIdx) -> spawn nodeId (slaveClosure slaveIdx )) $ zip nodeIds [0..]
  liftIO $ print slavePids

  let slaveInfos = M.fromList $ L.zipWith (\pid i -> (i, MkSlaveInfo i pid)) slavePids [0..]
  localNode  <- liftIO $ newLocalNode backend
  let rpcState = MkRpcState slaveInfos Nothing localNode
  liftIO $ do
    (a, b) <- runStdoutLoggingT $ runRec rpcState jobDesc
    k a b
  where
  runRec rpc (jobDesc@MkJobDesc {..}) = do
    (e, count) <- runStateT (expGen seed) 0
    infos <- execStateT (analyseLocal e) M.empty
    (infos', e') <- if shouldOptimize
                      then runStdoutLoggingT $ optimize count infos e
                      else return (infos, e)
    rpc' <- liftIO $ setSeed rpc seed
    (a', b) <- evalStateT (runSimpleLocal infos' e') (rpc', V.empty)
    case recPredicate a' of
      True -> return (a', b)
      False -> do let jobDesc' = jobDesc {seed = a'}
                  runRec rpc' jobDesc'



runRpc :: forall a b. (S.Serialize a, S.Serialize b, RemoteClass RpcState a) =>
  RemoteTable
  -> [String]
  -> JobDesc a b
  -> (Int -> Closure (Process()))
  -> (a -> b -> IO())
  -> IO()
runRpc rtable args jobDesc slaveClosure k = do
  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port rtable
      startMaster backend  (startClientRpc jobDesc slaveClosure k backend)
      putStrLn ("End")
    ["slave", host, port] -> do
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> putStrLn ("Bad args: " ++ show args)

