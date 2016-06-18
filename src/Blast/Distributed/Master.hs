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
(
  runLocal
)
where


import            Control.Concurrent.Async
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.ByteString as BS
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import qualified  Data.Vector as Vc

import Blast.Types
import Blast.Distributed.Types
import Blast.Common.Analyser
import Blast.Master.Analyser


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

runRemoteOneSlave ::(CommandClass s x, MonadIO m) => Int -> MExp 'Remote a -> StateT (s x , V.Vault) m ()
runRemoteOneSlave slaveId oe@(MRApply n (ExpClosure ce _) e) = do
  s <- getRemote
  r <- liftIO $ exec s slaveId n
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
          let csBs = p Vc.! slaveId
          liftIO $ print("cache missed", ceId)
          _ <- liftIO $ cache s slaveId ceId csBs
          runRemoteOneSlave slaveId oe
    RemCsResCacheMiss CachedArg -> do
      runRemoteOneSlave slaveId e
      runRemoteOneSlave slaveId oe
      -- todo uncache e if should not be cached
    ExecRes -> return ()
    ExecResError err -> error ( "remote call: " ++ err)

runRemoteOneSlave slaveId (MRConst n key _) = do
  s <- getRemote
  vault <- getVault
  case V.lookup key vault of
    Just partition -> do
      let bs = partition Vc.! slaveId
      _ <- liftIO $ cache s slaveId n bs
      return ()
    Nothing ->  error "MRConst value not cached"


fetchOneSlaveResults :: forall a s x m.
  (S.Serialize a, CommandClass s x, MonadIO m)
  => Int -> MExp 'Remote a -> StateT (s x, V.Vault) m a
fetchOneSlaveResults slaveId e = do
  s <- getRemote
  let n = getRemoteIndex e
  (rM::Either String a) <- liftIO $ fetch s slaveId n
  case rM of
    Right r -> return r
    Left err -> error ("Cannot fetch results: "++ show err)

fetchResults :: (S.Serialize r, MonadIO m, UnChunkable r, CommandClass s x) =>
  MExp 'Remote r -> StateT (s x, V.Vault) m r
fetchResults e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (fetchOneSlaveResults slaveId e) (s, vault)) slaveIds
  return $ unChunk r


runRemote ::(CommandClass s x, MonadLoggerIO m) => MExp 'Remote a -> StateT (s x, V.Vault) m ()
runRemote oe@(MRApply n (ExpClosure ce _) e) = do
  liftIO $ print ("running MRApply ", n)
  s <- getRemote
  runRemote e
  cp <- runLocal ce
  case cp of
    (_, Just _) -> return ()
    (c, Nothing) -> do
        let nbSlaves = getNbSlaves s
        let partition = fmap S.encode $ chunk' nbSlaves c
        vault <- getVault
        let key = getLocalVaultKey ce
        setVault (V.insert key (c, Just partition) vault)
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runRemoteOneSlave slaveId oe) (s, vault)) slaveIds
  return ()


runRemote e@(MRConst _ key a) = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  let partition = fmap S.encode $ chunk nbSlaves a
  let vault' = V.insert key partition vault
  setVault vault'
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runRemoteOneSlave slaveId e) (s, vault')) slaveIds
  return ()


runLocal ::(CommandClass s x, MonadLoggerIO m) => MExp 'Local a -> StateT (s x, V.Vault) m (a, Maybe (Partition BS.ByteString))
runLocal (MLConst _ key a) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just cv -> return cv
    Nothing -> do
      setVault (V.insert key (a, Nothing) vault)
      return (a, Nothing)

runLocal (MCollect _ key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      runRemote e
      a <- fetchResults e
      vault' <- getVault
      setVault (V.insert key (a, Nothing) vault')
      return (a, Nothing)

runLocal (MLApply n key f e) = do
  liftIO $ print ("running MLApply ", n)
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      (a, _) <- runLocal e
      (f', _) <-  runLocal f
      let r = f' a
      vault' <- getVault
      setVault (V.insert key (r, Nothing) vault')
      return (r, Nothing)



