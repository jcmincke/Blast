{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Runner.Local
(
  createController
  , runRec
)
where

import Debug.Trace
import            Control.Concurrent
import            Control.Concurrent.Async
import            Control.DeepSeq
import            Control.Monad
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.Map as M
import            Data.Maybe (fromJust)
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V

import            System.Random

import            Blast.Types
import            Blast.Common.Analyser
import            Blast.Master.Analyser as Ma
import            Blast.Master.Optimizer as Ma
import            Blast.Distributed.Types
import            Blast.Distributed.Master
import            Blast.Distributed.Slave


runRec :: forall a b m s.
  (S.Serialize a, S.Serialize b, CommandClass s a, MonadLoggerIO m) =>
  Config -> s a -> JobDesc a b -> m (a, b)
runRec config@(MkConfig {..}) s (jobDesc@MkJobDesc {..}) = do
  ((e::MExp 'Local (a,b)), count) <- runStateT (build (expGen seed)) 0
  liftIO $ print ("nb nodes (master) = ", count)
  infos <- execStateT (Ma.analyseLocal e) M.empty
  (infos2, e') <- if shouldOptimize
                    then trace ("MASTER OPTIMIZED") $ runStdoutLoggingT $ Ma.optimize count e
                    else return (infos, e)
  liftIO $ print $ M.keys infos2
  s' <- liftIO $ setSeed s seed
  ((a, b), _) <- evalStateT (runLocal e') (s', V.empty)
  a' <- liftIO $ reportingAction a b
  case recPredicate seed a' b of
    True -> return (a', b)
    False -> runRec config s' (jobDesc {seed = a'})


data RemoteChannels = MkRemoteChannels {
  iocOutChan :: Chan LocalSlaveRequest
  ,iocInChan :: Chan LocalSlaveResponse
}

data Controller a = MkController {
  slaveChannels :: M.Map Int RemoteChannels
  , seedM :: Maybe a
  , config :: Config
}

randomSlaveReset :: (S.Serialize a) => Controller a -> Int -> IO ()
randomSlaveReset s@(MkController {config = MkConfig {..}}) slaveId = do
  r <- randomRIO (0.0, 1.0)
  when (r > slaveAvailability) $ reset s slaveId



instance (S.Serialize a) => CommandClass Controller a where
  getNbSlaves (MkController {..}) = M.size slaveChannels
  status (MkController {..}) slaveId = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    writeChan iocOutChan LsReqStatus
    (LsRespBool b) <- readChan iocInChan
    return b
  exec s@(MkController {..}) slaveId i = do
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
  cache (MkController {..}) slaveId i bs = do
    let (MkRemoteChannels {..}) = trace (show ("caching for ", i)) $ slaveChannels M.! slaveId
   -- let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqCache i bs
    let !req' = force req
    writeChan iocOutChan req'
    r <- readChan iocInChan
    case r of
      (LsRespBool b) -> return b
      (LsRespError s) -> do
        putStrLn $ "Erreur: " ++ s
        return False
      (LsRespVoid) -> do
        putStrLn $ "LsRespVoid"
        error "e"
      (LsFetch _) -> do
        putStrLn $ "LsFetch"
        error "e"
      (LocalSlaveExecuteResult x) -> do
        putStrLn $ "LocalSlaveExecuteResult "++ show x
        error "e"


  uncache (MkController {..}) slaveId i = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqUncache i
    let !req' = force req
    writeChan iocOutChan req'
    (LsRespBool b) <- readChan iocInChan
    return b
  fetch (MkController {..}) slaveId i = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqFetch i
    let !req' = force req
    writeChan iocOutChan req'
    (LsFetch bsM) <- readChan iocInChan
    case bsM of
      Just bs -> return $ S.decode bs
      Nothing -> return $ Left "Cannot fetch result"

  reset (MkController {..}) slaveId = do
    runStdoutLoggingT $ $(logInfo) $ T.pack ("Resetting node  " ++ show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqReset (S.encode $ fromJust seedM)
    writeChan iocOutChan req
    LsRespVoid <- readChan iocInChan
    return ()
  setSeed s@(MkController {..}) a = do
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


createController :: (S.Serialize a, MonadIO m, MonadLoggerIO m, m ~ LoggingT IO) =>
      Config -> Int -> JobDesc a b
      -> m (Controller a)
createController cf@(MkConfig {..}) nbSlaves (MkJobDesc {..}) = do
  m <- liftIO $ foldM proc M.empty [0..nbSlaves-1]
  return $ MkController m Nothing cf
  where
  expGen' a = build $ expGen a
  proc acc i = do
    (iChan, oChan, ls) <- createOneSlave i M.empty
    let rc = MkRemoteChannels iChan oChan
    _ <- (forkIO $ runSlave iChan oChan ls)
    return $ M.insert i rc acc

  createOneSlave slaveId infos = do
    iChan <- newChan
    oChan <- newChan
    return $ (iChan, oChan, MkLocalSlave slaveId infos V.empty expGen' cf)


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






