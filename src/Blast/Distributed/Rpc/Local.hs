

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
  Config -> s a -> JobDesc m a b -> m (a, b)
runSimpleLocalRec config@(MkConfig {..}) s (jobDesc@MkJobDesc {..}) = do
  (e, count) <- runStateT (expGen seed) 0
  infos <- execStateT (analyseLocal e) M.empty
  (infos', e') <- if shouldOptimize
                    then runStdoutLoggingT $ optimize count infos e
                    else return (infos, e)
  s' <- liftIO $ setSeed s seed
  ((a, b), _) <- evalStateT (runSimpleLocal infos' e') (s', V.empty)
  a' <- liftIO $ reportingAction a b
  case recPredicate a' of
    True -> return (a', b)
    False -> runSimpleLocalRec config s' (jobDesc {seed = a'})


data RemoteChannels = MkRemoteChannels {
  iocOutChan :: Chan LocalSlaveRequest
  ,iocInChan :: Chan LocalSlaveResponse
}

data SimpleRemote a = MkSimpleRemote {
  slaveChannels :: M.Map Int RemoteChannels
  , seedM :: Maybe a
  , config :: Config
}

randomSlaveReset :: (S.Serialize a) => SimpleRemote a -> Int -> IO ()
randomSlaveReset s@(MkSimpleRemote {config = MkConfig {..}}) slaveId= do
  r <- randomRIO (0.0, 1.0)
  when (r > slaveAvailability) $ reset s slaveId



instance (S.Serialize a) => RemoteClass SimpleRemote a where
  getNbSlaves (MkSimpleRemote {..}) = M.size slaveChannels
  status (MkSimpleRemote {..}) slaveId = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    writeChan iocOutChan LsReqStatus
    (LsRespBool b) <- readChan iocInChan
    return b
  execute s@(MkSimpleRemote {..}) slaveId i = do
    randomSlaveReset s slaveId
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqExecute i
    let !req' = force req
    writeChan iocOutChan req'
    (LocalSlaveExecuteResult resp) <- readChan iocInChan
    case resp of
      RemCsResCacheMiss t -> return $ RemCsResCacheMiss t
      ExecRes -> return ExecRes
      ExecResError err -> return (ExecResError err)
  cache (MkSimpleRemote {..}) slaveId i bs = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
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
  fetch (MkSimpleRemote {..}) slaveId i = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqFetch i
    let !req' = force req
    writeChan iocOutChan req'
    (LsFetch bsM) <- readChan iocInChan
    case bsM of
      Just bs -> return $ S.decode bs
      Nothing -> return $ Left "Cannot fetch result"

  reset (MkSimpleRemote {..}) slaveId = do
    runStdoutLoggingT $ $(logInfo) $ T.pack ("Resetting node  " ++ show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqReset (S.encode $ fromJust seedM)
    writeChan iocOutChan req
    LsRespVoid <- readChan iocInChan
    return ()
  setSeed s@(MkSimpleRemote {..}) a = do
    let s' = s {seedM = Just a}
    resetAll s'
    return s'
    where
    resetAll as = do
      let nbSlaves = getNbSlaves as
      let slaveIds = [0 .. nbSlaves - 1]
      _ <- mapConcurrently (\slaveId -> reset as slaveId) slaveIds
      return ()
  stop _ = return ()


createSimpleRemote :: (S.Serialize a, MonadIO m, MonadLoggerIO m, m ~ LoggingT IO) =>
      Config -> Int -> (a -> StateT Int m (LocalExp (a, b)))
      -> m (SimpleRemote a)
createSimpleRemote cf@(MkConfig {..}) nbSlaves expGen = do
  m <- liftIO $ foldM proc M.empty [0..nbSlaves-1]
  return $ MkSimpleRemote m Nothing cf
  where
  proc acc i = do
    (iChan, oChan, ls) <- createOneSlave i M.empty
    let rc = MkRemoteChannels iChan oChan
    _ <- (forkIO $ runSlave iChan oChan ls)
    return $ M.insert i rc acc

  createOneSlave slaveId infos = do
    iChan <- newChan
    oChan <- newChan
    return $ (iChan, oChan, MkLocalSlave slaveId infos V.empty expGen cf)


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






