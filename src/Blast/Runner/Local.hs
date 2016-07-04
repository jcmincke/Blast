{-|
Module      : Blast.Syntax
Copyright   : (c) Jean-Christophe Mincke, 2016
License     : BSD3
Maintainer  : jeanchristophe.mincke@gmail.com
Stability   : experimental
Portability : POSIX

-}

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
  runRec
)
where

--import Debug.Trace
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

import            Blast
import            Blast.Distributed.Interface


-- | Runs a computation locally.
-- Uses threads to distribute the remote computations.
runRec :: forall a b.
  (S.Serialize a, S.Serialize b) =>
  Int                       -- ^ Number of slaves.
  -> Config                 -- ^ Configuration.
  -> JobDesc a b            -- ^ Job to execute.
  -> LoggingT IO (a, b)     -- ^ Results.
runRec nbSlaves config jobDesc = do
  controller <- createController config nbSlaves jobDesc
  doRunRec config controller jobDesc


doRunRec :: forall a b m.
  (S.Serialize a, S.Serialize b, MonadLoggerIO m) =>
  Config -> Controller a -> JobDesc a b -> m (a, b)
doRunRec config@(MkConfig {..}) s (jobDesc@MkJobDesc {..}) = do
  let program = computationGen seed
  (refMap, count) <- generateReferenceMap 0 M.empty program
  e <- build shouldOptimize refMap (0::Int) count program
  infos <- execStateT (analyseLocal e) M.empty
  s' <- liftIO $ setSeed' s seed
  ((a, b), _) <- evalStateT (runLocal e) (s', V.empty, infos)
  a' <- liftIO $ reportingAction a b
  case recPredicate seed a' b of
    True -> return (a', b)
    False -> doRunRec config s' (jobDesc {seed = a'})


data RemoteChannels = MkRemoteChannels {
  iocOutChan :: Chan LocalSlaveRequest
  ,iocInChan :: Chan LocalSlaveResponse
}

data Controller a = MkController {
  slaveChannels :: M.Map Int RemoteChannels
  , seedM :: Maybe a
  , config :: Config
  , statefullSlaveMode :: Bool
}


randomSlaveReset :: (S.Serialize a) => Controller a -> Int -> IO ()
randomSlaveReset s@(MkController {config = MkConfig {..}, seedM = seedM}) slaveId = do
  case seedM of
    Just a -> do
      r <- randomRIO (0.0, 1.0)
      when (r > slaveAvailability) $ do
        let req = resetCommand (S.encode a)
        send s slaveId req
        return ()


instance (S.Serialize a) => CommandClass' Controller a where
  isStatefullSlave' (MkController{ statefullSlaveMode = mode }) = mode
  getNbSlaves' (MkController {..}) = M.size slaveChannels

  send s@(MkController {..}) slaveId req = do
    randomSlaveReset s slaveId
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let !req' = force req
    writeChan iocOutChan req'
    resp <- readChan iocInChan
    return $ Right resp

  stop' _ = return ()
  setSeed' s@(MkController {..}) a = do
    let s' = s {seedM = Just a}
    resetAll s'
    return s'
    where
    resetAll as = do
      let nbSlaves = getNbSlaves' as
      let slaveIds = [0 .. nbSlaves - 1]
      let req = resetCommand (S.encode a)
      _ <- mapConcurrently (\slaveId -> send as slaveId req) slaveIds
      return ()

{-}
instance (S.Serialize a) => CommandClass Controller a where
  isStatefullSlave (MkController{ statefullSlaveMode = mode }) = mode
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
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqCache i bs
    let !req' = force req
    writeChan iocOutChan req'
    r <- readChan iocInChan
    case r of
      LsRespBool b -> return b
      _ -> error "Should not reach here"

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
  batch (MkController {..}) slaveId nRet requests = do
    let req = LsReqBatch nRet (LsReqReset (S.encode $ fromJust seedM) : requests)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    writeChan iocOutChan req
    (LsRespBatch rE) <- readChan iocInChan
    case rE of
      Right bs -> return $ S.decode bs
      Left e -> error ("batch, slave "++show slaveId ++ " " ++ e)
-}

createController :: (S.Serialize a) =>
      Config -> Int -> JobDesc a b
      -> LoggingT IO (Controller a)
createController cf@(MkConfig {..}) nbSlaves (MkJobDesc {..}) = do
  m <- liftIO $ foldM proc M.empty [0..nbSlaves-1]
  return $ MkController m Nothing cf statefullSlaves
  where
  proc acc i = do
    (iChan, oChan, ls) <- createOneSlave i M.empty
    let rc = MkRemoteChannels iChan oChan
    _ <- (forkIO $ runSlave iChan oChan ls)
    return $ M.insert i rc acc

  createOneSlave slaveId infos = do
    iChan <- newChan
    oChan <- newChan
    return $ (iChan, oChan, MkSlaveContext slaveId infos V.empty computationGen cf)


runSlave :: (S.Serialize a) => Chan LocalSlaveRequest -> Chan LocalSlaveResponse -> SlaveContext (LoggingT IO) a b -> IO ()
runSlave inChan outChan als =
  runStdoutLoggingT $ go als
  where
  go ls@(MkSlaveContext {..}) = do
    req <- liftIO $ readChan inChan
    (resp, ls') <- runCommand req ls
    let resp' = force resp
    liftIO $ writeChan outChan resp'
    go ls'



{-
runCommand' :: forall a b m. (S.Serialize a, MonadLoggerIO m) => LocalSlaveRequest -> LocalSlave m a b -> m (LocalSlaveResponse, LocalSlave m a b)

runCommand' (LsReqReset  bs) ls = do
  ls' <- slaveReset bs ls
  return  (LsRespVoid, ls')

runCommand' LsReqStatus ls = do
  (b, ls') <- slaveStatus ls
  return (LsRespBool b, ls')

runCommand' (LsReqExecute i) ls = do
    (res, ls') <- slaveExecute i ls
    return (LocalSlaveExecuteResult res, ls')

runCommand' (LsReqCache i bs)  ls = do
  (res , ls') <- slaveCache i bs ls
  case res of
    Nothing -> (LsRespBool True, ls')
    Just err -> (LocalSlaveExecuteResult (ExecResError err), ls')

runCommand' (LsReqUncache i) ls = do
  (res , ls') <- slaveUnCache i ls
  case res of
    Nothing -> (LsRespBool True, ls')
    Just err -> (LocalSlaveExecuteResult (ExecResError err), ls')

runCommand' i ls = do
    (res, ls') <- slaveFetch i ls
    return $ (LsFetch res, ls')

runCommand' (LsReqBatch nRes requests) ls = do
  ls' <- foldM (\acc req -> do  (_, acc') <- runCommand' req acc
                                return acc') ls requests
  -- fetch results
  (res, ls'') <- runCommand' ls' (LsReqFetch nRes)
  case res of
    (LsFetch (Just r)) -> return $ (LsRespBatch (Right r), ls'')
    (LsFetch Nothing) -> return $ (LsRespBatch (Left "Batch: could not read results"), ls'')
    _ -> return $ (LsRespBatch (Left "Batch: bad response"), ls'')
-}





