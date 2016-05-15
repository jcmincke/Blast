

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Distributed.Rpc.Local

where

import            Control.Concurrent
import            Control.Concurrent.Async
import            Control.DeepSeq
import            Control.Monad
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import            Data.Maybe (fromJust)
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V

import            System.Random

import Blast.Types
import Blast.Analyser
import Blast.Optimizer
import Blast.Distributed.Types
import Blast.Distributed.Master
import Blast.Distributed.Slave


runSimpleLocalRec ::
  (S.Serialize a, S.Serialize b, RemoteClass s a, MonadIO m, MonadLoggerIO m) =>
  s a -> Bool -> (a -> StateT Int m (LocalExp (a, b))) -> a -> (a -> Bool) -> m (a, b)
runSimpleLocalRec s shouldOptimize gen a predicate = do
  (e, count) <- runStateT (gen a) 0
  infos <- execStateT (analyseLocal e) M.empty
  (infos', e') <- if shouldOptimize
                    then runStdoutLoggingT $ optimize count infos e
                    else return (infos, e)
  s' <- liftIO $ setSeed s a
  (a', b) <- evalStateT (runSimpleLocal infos' e') (s', V.empty)
  case predicate a' of
    True -> return (a', b)
    False -> runSimpleLocalRec s' shouldOptimize gen a' predicate


data RemoteChannels = MkRemoteChannels {
  iocOutChan :: Chan LocalSlaveRequest
  ,iocInChan :: Chan LocalSlaveResponse
}

data SimpleRemote a = MkSimpleRemote {
  slaveChannels :: M.Map Int RemoteChannels
  , availabilityProb :: Float
  , seed :: Maybe a
  , shouldOptimize :: Bool
}

randomSlaveReset :: (S.Serialize a) => SimpleRemote a -> Int -> IO ()
randomSlaveReset s@(MkSimpleRemote {..}) slaveId= do
  r <- randomRIO (0.0, 1.0)
  when (r > availabilityProb) $ reset s slaveId



instance (S.Serialize a) => RemoteClass SimpleRemote a where
  getNbSlaves (MkSimpleRemote {..}) = M.size slaveChannels
  status (MkSimpleRemote {..}) slaveId = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    writeChan iocOutChan LsReqStatus
    (LsRespBool b) <- readChan iocInChan
    return b
  execute s@(MkSimpleRemote {..}) slaveId i c a (ResultDescriptor sr sc) = do
    randomSlaveReset s slaveId
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let ec = encodeRemoteValue c
    let ea = encodeRemoteValue a
    let req = LsReqExecute i ec ea (ResultDescriptor sr sc)
    let !req' = force req
    writeChan iocOutChan req'
    (LocalSlaveExecuteResult resp) <- readChan iocInChan
    case resp of
      RemCsResCacheMiss t -> return $ RemCsResCacheMiss t
      ExecRes Nothing -> return (ExecRes Nothing)
      ExecRes (Just bs) -> do
        case S.decode bs of
          Left err -> error ("decode failed: " ++ err)
          Right r -> return (ExecRes $ Just r)
      ExecResError err -> return (ExecResError err)
  cache (MkSimpleRemote {..}) slaveId i a = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let bs = S.encode a
    let req = LsReqCache i bs
    let !req' = force req
    writeChan iocOutChan req'
    (LsRespBool b) <- readChan iocInChan
    return b
  uncache (MkSimpleRemote {..}) slaveId i = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqUncache i
    let !req' = force req
    writeChan iocOutChan req'
    (LsRespBool b) <- readChan iocInChan
    return b
  isCached (MkSimpleRemote {..}) slaveId i = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqIsCached i
    let !req' = force req
    writeChan iocOutChan req'
    (LsRespBool b) <- readChan iocInChan
    return b
  reset (MkSimpleRemote {..}) slaveId = do
    runStdoutLoggingT $ $(logInfo) $ T.pack ("Resetting node  " ++ show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqReset shouldOptimize (S.encode $ fromJust seed)
    writeChan iocOutChan req
    LsRespVoid <- readChan iocInChan
    return ()
  setSeed s@(MkSimpleRemote {..}) a = do
    let s' = s {seed = Just a}
    resetAll s'
    return s'
    where
    resetAll as = do
      let nbSlaves = getNbSlaves as
      let slaveIds = [0 .. nbSlaves - 1]
      _ <- mapConcurrently (\slaveId -> reset as slaveId) slaveIds
      return ()
  stop _ = return ()


encodeRemoteValue :: (S.Serialize a) => RemoteValue a -> RemoteValue BS.ByteString
encodeRemoteValue (RemoteValue a) = RemoteValue $ S.encode a
encodeRemoteValue CachedRemoteValue = CachedRemoteValue


createSimpleRemote :: (S.Serialize a, MonadIO m, MonadLoggerIO m, m ~ LoggingT IO) =>
      Float -> Bool -> Int -> (a -> StateT Int m (LocalExp (a, b)))
      -> m (SimpleRemote a)
createSimpleRemote slaveAvailability shouldOptimize nbSlaves expGen = do
  m <- liftIO $ foldM proc M.empty [0..nbSlaves-1]
  return $ MkSimpleRemote m slaveAvailability Nothing shouldOptimize
  where
  proc acc i = do
    (iChan, oChan, ls) <- createOneSlave i M.empty
    let rc = MkRemoteChannels iChan oChan
    _ <- (forkIO $ runSlave iChan oChan ls)
    return $ M.insert i rc acc

  createOneSlave slaveId infos = do
    iChan <- newChan
    oChan <- newChan
    return $ (iChan, oChan, MkLocalSlave slaveId infos V.empty expGen)


runSlave :: (S.Serialize a) => Chan LocalSlaveRequest -> Chan LocalSlaveResponse -> LocalSlave (LoggingT IO) a b -> IO ()
runSlave inChan outChan als =
  runStdoutLoggingT $ go als
  where
  go ls@(MkLocalSlave {..}) = do
    req <- liftIO $ readChan inChan
    (resp, ls') <- runCommand ls req
    let resp' = force resp
    liftIO $ writeChan outChan resp'
    go ls'






