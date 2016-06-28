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
import            Control.Monad
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Set as S
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

getRemote :: (Monad m) => StateT (s, V.Vault, InfoMap) m s
getRemote = do
  (s, _, _) <- get
  return s

getVault :: (Monad m) => StateT (s, V.Vault, InfoMap) m (V.Vault)
getVault = do
  (_, vault, _) <- get
  return vault

setVault :: (Monad m) => V.Vault -> StateT (s, V.Vault, InfoMap) m ()
setVault vault= do
  (s, _, m) <- get
  put (s, vault, m)

dereference :: (Monad m) => Int -> Int -> StateT (s, V.Vault, InfoMap) m Bool
dereference parent child = do
  (s, vault, m) <- get
  let (GenericInfo crefs i) = m M.! child
  if S.member parent crefs
    then do
      let crefs' = S.delete parent crefs
      let m' = M.insert child (GenericInfo crefs' i) m
      put (s, vault, m')
      return $ S.null crefs'
    else error ("Remove parent reference twice for parent "++show parent ++" and child "++show child)


runRemoteOneSlaveStatefull ::(CommandClass s x, MonadIO m) => Int -> MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m ()
runRemoteOneSlaveStatefull slaveId oe@(MRApply n (ExpClosure ce _) e) = do
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
          _ <- liftIO $ cache s slaveId ceId csBs
          runRemoteOneSlaveStatefull slaveId oe
    RemCsResCacheMiss CachedArg -> do
      runRemoteOneSlaveStatefull slaveId e
      runRemoteOneSlaveStatefull slaveId oe
      -- todo uncache e if should not be cached
    ExecRes -> return ()
    ExecResError err -> error ( "remote call: " ++ err)

runRemoteOneSlaveStatefull slaveId (MRConst n key _) = do
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
  => Int -> MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m a
fetchOneSlaveResults slaveId e = do
  s <- getRemote
  let n = getRemoteIndex e
  (rM::Either String a) <- liftIO $ fetch s slaveId n
  case rM of
    Right r -> return r
    Left err -> error ("Cannot fetch results: "++ show err)

fetchResults :: (S.Serialize r, MonadIO m, UnChunkable r, CommandClass s x) =>
  MExp 'Remote r -> StateT (s x, V.Vault, InfoMap) m r
fetchResults e = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (fetchOneSlaveResults slaveId e) st) slaveIds
  return $ unChunk r


unCacheRemoteOneSlave :: (CommandClass s x, MonadIO m) => Int -> MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m Bool
unCacheRemoteOneSlave slaveId e = do
  s <- getRemote
  liftIO $ uncache s slaveId (getRemoteIndex e)

unCacheRemote :: (CommandClass s x, MonadIO m) => MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m ()
unCacheRemote e = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (unCacheRemoteOneSlave slaveId e) st) slaveIds
  -- todo error handling
  return ()

unCacheLocalOneSlave :: (CommandClass s x, MonadIO m) => Int -> MExp 'Local a -> StateT (s x , V.Vault, InfoMap) m Bool
unCacheLocalOneSlave slaveId e = do
  s <- getRemote
  liftIO $ uncache s slaveId (getLocalIndex e)

unCacheLocal :: (CommandClass s x, MonadIO m) => MExp 'Local a -> StateT (s x , V.Vault, InfoMap) m ()
unCacheLocal e = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (unCacheLocalOneSlave slaveId e) st) slaveIds
  -- todo error handling
  return ()

runRemote ::(CommandClass s x, MonadLoggerIO m, S.Serialize a, UnChunkable a) => MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m a
runRemote e = do
  prepareRunRemote e
  s <- getRemote
  case statefullSlaveMode s of
    True -> do
      doRunRemoteStatefull e
      fetchResults e
    False -> doRunRemoteStateless e

doRunRemoteStatefull ::(CommandClass s x, MonadLoggerIO m) => MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m ()
doRunRemoteStatefull oe@(MRApply n (ExpClosure ce _) e) = do
  s <- getRemote
  doRunRemoteStatefull e
  cp <- runLocal ce
  case cp of
    (_, Just _) -> return ()
    (c, Nothing) -> do
        let nbSlaves = getNbSlaves s
        let partition = fmap S.encode $ chunk' nbSlaves c
        vault <- getVault
        let key = getLocalVaultKey ce
        setVault (V.insert key (c, Just partition) vault)
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runRemoteOneSlaveStatefull slaveId oe) st) slaveIds

  -- dereference children and cleanup remote cache if necessary
  cleanupCacheE <- dereference n (getRemoteIndex e)
  when cleanupCacheE $ do unCacheRemote e

  cleanupCacheCe <- dereference n (getLocalIndex ce)
  when cleanupCacheCe $ do unCacheLocal ce
  return ()


doRunRemoteStatefull e@(MRConst _ key a) = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  let partition = fmap S.encode $ chunk nbSlaves a
  let vault' = V.insert key partition vault
  setVault vault'
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runRemoteOneSlaveStatefull slaveId e) st) slaveIds
  return ()

doRunRemoteStateless ::(CommandClass s x, MonadLoggerIO m) => MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m a
doRunRemoteStateless = undefined

-- evaluate all local values (captured in closures)
prepareRunRemote ::(CommandClass s x, MonadLoggerIO m) => MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m ()
prepareRunRemote oe@(MRApply n (ExpClosure ce _) e) = do
  s <- getRemote
  prepareRunRemote e
  cp <- runLocal ce
  case cp of
    (_, Just _) -> return ()
    (c, Nothing) -> do
        let nbSlaves = getNbSlaves s
        let partition = fmap S.encode $ chunk' nbSlaves c
        vault <- getVault
        let key = getLocalVaultKey ce
        setVault (V.insert key (c, Just partition) vault)
  return ()


prepareRunRemote e@(MRConst _ key a) = return ()

runLocal ::(CommandClass s x, MonadLoggerIO m) => MExp 'Local a -> StateT (s x, V.Vault, InfoMap) m (a, Maybe (Partition BS.ByteString))
runLocal (MLConst _ key a) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just cv -> return cv
    Nothing -> do
      setVault (V.insert key (a, Nothing) vault)
      return (a, Nothing)

runLocal (MCollect n key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      a <- runRemote e
      vault' <- getVault
      setVault (V.insert key (a, Nothing) vault')

        -- dereference children and clenup remote cache if necessary
      cleanupCacheE <- dereference n (getRemoteIndex e)
      when cleanupCacheE $ do unCacheRemote e
      return (a, Nothing)

runLocal (MLApply _ key f e) = do
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



