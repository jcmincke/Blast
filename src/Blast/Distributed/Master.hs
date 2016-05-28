{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Distributed.Master

where

import            Control.Concurrent.Async
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.List as L
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V

import Blast.Internal.Types
import Blast.Distributed.Types
import Blast.Analyser




getRemote :: (Monad m) => StateT (s, V.Vault) m s
getRemote = do
  (s, _) <- get
  return s

getVault :: (Monad m) => StateT (s, V.Vault) m (V.Vault)
getVault = do
  (_, vault) <- get
  return vault

setVault :: (Monad m) => V.Vault -> StateT (s, V.Vault) m ()
setVault vault= do
  (s, _) <- get
  put (s, vault)


createRemoteCachedRemoteValue :: RemoteExp a -> RemoteValue a
createRemoteCachedRemoteValue _ = CachedRemoteValue

createLocalCachedRemoteValue :: LocalExp a -> RemoteValue a
createLocalCachedRemoteValue _ = CachedRemoteValue

runSimpleRemoteOneSlaveNoRet ::(S.Serialize a, RemoteClass s x, MonadIO m) => Int -> InfoMap -> RemoteExp a -> StateT (s x , V.Vault) m ()
runSimpleRemoteOneSlaveNoRet slaveId m oe@(RMap n _ (ExpClosure ce _) e) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (createLocalCachedRemoteValue ce) (createRemoteCachedRemoteValue e) (ResultDescriptor False True :: ResultDescriptor ())
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cache while executing remote on one slave"
        Just c -> do
          let ceId = getLocalIndex ce
          _ <- liftIO $ cache s slaveId ceId c
          runSimpleRemoteOneSlaveNoRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just _) -> error "no remote value should be returned"

runSimpleRemoteOneSlaveNoRet slaveId _ (RConst n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let subRdd = chunk nbSlaves a L.!! slaveId
  _ <- liftIO $ cache s slaveId n subRdd
  return ()



runSimpleRemoteOneSlaveRet ::(S.Serialize a, RemoteClass s x, MonadIO m) => Int -> InfoMap -> RemoteExp a -> StateT (s x, V.Vault) m a


runSimpleRemoteOneSlaveRet slaveId m oe@(RMap n _ (ExpClosure ce _) e) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (createLocalCachedRemoteValue ce) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cache while executing remote on one slave 3"
        Just c -> do
          let cId = getLocalIndex ce
          _ <- liftIO $ cache s slaveId cId c
          runSimpleRemoteOneSlaveRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId _rr (RConst n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let subRdd = chunk nbSlaves a L.!! slaveId
  _ <- liftIO $ cache s slaveId n subRdd
  return subRdd


runSimpleRemoteRet ::(S.Serialize a, Chunkable a, RemoteClass s x, MonadLoggerIO m) => InfoMap -> RemoteExp a -> StateT (s x, V.Vault) m a
runSimpleRemoteRet m oe@(RMap _ _ (ExpClosure ce _) _) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m oe) (s, vault)) slaveIds
  return $ unChunk r


runSimpleRemoteRet m e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m e) (s, vault)) slaveIds
  return $ unChunk r



runSimpleRemoteNoRet ::(S.Serialize a, Chunkable a, RemoteClass s x, MonadLoggerIO m) => InfoMap -> RemoteExp a -> StateT (s x, V.Vault) m ()

runSimpleRemoteNoRet m oe@(RMap _ _ (ExpClosure ce _) _) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveNoRet slaveId m oe) (s, vault)) slaveIds
  return ()



runSimpleRemoteNoRet m e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveNoRet slaveId m e) (s, vault)) slaveIds
  return ()


runLocalFun :: (RemoteClass s x, MonadLoggerIO m) => InfoMap -> ExpClosure a b -> StateT (s x, V.Vault) m (a -> IO b)
runLocalFun m (ExpClosure e f) = do
  e' <- runSimpleLocal m e
  return $ f e'

runSimpleLocal ::(RemoteClass s x, MonadLoggerIO m) => InfoMap -> LocalExp a -> StateT (s x, V.Vault) m a
runSimpleLocal _ (LConst _ key a) = do
      vault <- getVault
      setVault (V.insert key a vault)
      return a

runSimpleLocal m (Collect _ key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleRemoteRet m e
      vault' <- getVault
      setVault (V.insert key a vault')
      return a

runSimpleLocal m (FMap _ key f e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleLocal m e
      f' <-  runSimpleLocal m f
      let r = f' a
      vault' <- getVault
      setVault (V.insert key r vault')
      return r



