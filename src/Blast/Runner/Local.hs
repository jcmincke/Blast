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

import qualified  Data.Map as M
import qualified  Data.Serialize as S

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
  (a, b) <- runComputation config s jobDesc
  a' <- liftIO $ reportingAction a b
  case recPredicate seed a' b of
    True -> return (a', b)
    False -> doRunRec config s (jobDesc {seed = a'})


data RemoteChannels = MkRemoteChannels {
  iocOutChan :: Chan SlaveRequest
  ,iocInChan :: Chan SlaveResponse
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
        _ <- send s slaveId req
        return ()
    Nothing -> return ()

instance (S.Serialize a) => CommandClass Controller a where
  isStatefullSlave (MkController{ statefullSlaveMode = mode }) = mode
  getNbSlaves (MkController {..}) = M.size slaveChannels

  send s@(MkController {..}) slaveId req = do
    randomSlaveReset s slaveId
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let !req' = force req
    writeChan iocOutChan req'
    resp <- readChan iocInChan
    return $ Right resp

  stop _ = return ()
  setSeed s@(MkController {..}) a = do
    let s' = s {seedM = Just a}
    resetAll s'
    return s'
    where
    resetAll as = do
      let nbSlaves = getNbSlaves as
      let slaveIds = [0 .. nbSlaves - 1]
      let req = resetCommand (S.encode a)
      _ <- mapConcurrently (\slaveId -> send as slaveId req) slaveIds
      return ()

createController :: (S.Serialize a) =>
      Config -> Int -> JobDesc a b
      -> LoggingT IO (Controller a)
createController cf@(MkConfig {..}) nbSlaves jobDesc@(MkJobDesc {..}) = do
  m <- liftIO $ foldM proc M.empty [0..nbSlaves-1]
  return $ MkController m Nothing cf statefullSlaves
  where
  proc acc i = do
    (iChan, oChan, ls) <- createOneSlave i
    let rc = MkRemoteChannels iChan oChan
    _ <- (forkIO $ runSlave iChan oChan ls)
    return $ M.insert i rc acc

  createOneSlave slaveId = do
    iChan <- newChan
    oChan <- newChan
    return $ (iChan, oChan, makeSlaveContext cf slaveId jobDesc)


runSlave :: (S.Serialize a) => Chan SlaveRequest -> Chan SlaveResponse -> SlaveContext (LoggingT IO) a b -> IO ()
runSlave inChan outChan als =
  runStdoutLoggingT $ go als
  where
  go ls = do
    req <- liftIO $ readChan inChan
    (resp, ls') <- runCommand req ls
    let resp' = force resp
    liftIO $ writeChan outChan resp'
    go ls'





