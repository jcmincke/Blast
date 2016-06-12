{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Distributed.Master

where


import            Control.Concurrent.Async
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import qualified  Data.Vector as Vc

import Blast.Internal.Types
import Blast.Distributed.Types
import Blast.Common.Analyser
import Blast.Master.Analyser
import Blast.Master.Optimizer


getLocalVaultKey :: MExp 'Local a -> LocalKey a
getLocalVaultKey (MLConst _ k _) = k
getLocalVaultKey (MCollect _ k _) = k
getLocalVaultKey (MLApply _ k _ _) = k

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

runSimpleRemoteOneSlaveRMapNoRet ::(RemoteClass s x, MonadIO m) => Int -> InfoMap -> MExp 'Remote a -> StateT (s x , V.Vault) m ()
runSimpleRemoteOneSlaveRMapNoRet slaveId m oe@(MRApply n (ExpClosure ce _) e) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cached while executing remote on one slave"
        Just (_, Nothing) -> error "local value not cached (BS) while executing remote on one slave"
        Just (_, Just p) -> do
          let ceId = getLocalIndex ce
          let nbSlaves = getNbSlaves s
          let csBs = partitionToVector p Vc.! slaveId
          _ <- liftIO $ cache s slaveId ceId csBs
          runSimpleRemoteOneSlaveRMapNoRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      case e of
        (MRApply _ _ _) -> runSimpleRemoteOneSlaveRMapNoRet slaveId m e
        (MRConst _ _ _) -> runSimpleRemoteOneSlaveRConstNoRet slaveId m e
      runSimpleRemoteOneSlaveRMapNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes -> return ()
    ExecResError err -> error ( "remote call: " ++ err)

runSimpleRemoteOneSlaveRConstNoRet ::(S.Serialize a, RemoteClass s x, MonadIO m) => Int -> InfoMap -> MExp 'Remote a -> StateT (s x , V.Vault) m ()

runSimpleRemoteOneSlaveRConstNoRet slaveId _ (MRConst n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  -- todo chunk is applied for each slave , not efficient, to fix
  let subRdd = partitionToVector (chunk nbSlaves a) Vc.! slaveId
  let subRddBs = S.encode subRdd
  _ <- liftIO $ cache s slaveId n subRddBs
  return ()



fetchOnSlaveResults :: forall a s x m.
  (S.Serialize a, RemoteClass s x, MonadIO m)
  => Int -> InfoMap -> MExp 'Remote a -> StateT (s x, V.Vault) m a
fetchOnSlaveResults slaveId m e = do
  s <- getRemote
  let n = getRemoteIndex e
  (rM::Either String a) <- liftIO $ fetch s slaveId n
  case rM of
    Right r -> return r
    Left err -> error ("Cannot fetch results: "++ show err)

fetchResults m e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (fetchOnSlaveResults slaveId m e) (s, vault)) slaveIds
  return $ unChunk r


runSimpleRemoteNoRet ::(S.Serialize a, UnChunkable a, RemoteClass s x, MonadLoggerIO m) => InfoMap -> MExp 'Remote a -> StateT (s x, V.Vault) m ()

runSimpleRemoteNoRet m oe@(MRApply _ (ExpClosure ce _) _) = do
  s <- getRemote
  cp <- runSimpleLocal m ce
  case cp of
    (c, Just _) -> return ()
    (c, Nothing) -> do
        let nbSlaves = getNbSlaves s
        let partition = fmap S.encode $ chunk' nbSlaves c
        vault <- getVault
        let key = getLocalVaultKey ce
        setVault (V.insert key (c, Just partition) vault)
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRMapNoRet slaveId m oe) (s, vault)) slaveIds
  return ()



runSimpleRemoteNoRet m e@(MRConst _ _ _) = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRConstNoRet slaveId m e) (s, vault)) slaveIds
  return ()


runLocalFun :: (RemoteClass s x, MonadLoggerIO m) => InfoMap -> ExpClosure MExp a b -> StateT (s x, V.Vault) m (a -> IO b)
runLocalFun m (ExpClosure e f) = do
  (e', _) <- runSimpleLocal m e
  return $ f e'

runSimpleLocal ::(RemoteClass s x, MonadLoggerIO m) => InfoMap -> MExp 'Local a -> StateT (s x, V.Vault) m (a, Maybe (Partition BS.ByteString))
runSimpleLocal _ (MLConst _ key a) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      setVault (V.insert key (a, Nothing) vault)
      return (a, Nothing)

runSimpleLocal m (MCollect _ key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      runSimpleRemoteNoRet m e
      a <- fetchResults m e
      vault' <- getVault
      setVault (V.insert key (a, Nothing) vault')
      return (a, Nothing)

runSimpleLocal m (MLApply _ key f e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      (a, _) <- runSimpleLocal m e
      (f', _) <-  runSimpleLocal m f
      let r = f' a
      vault' <- getVault
      setVault (V.insert key (r, Nothing) vault')
      return (r, Nothing)



