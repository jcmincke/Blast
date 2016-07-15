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
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe (catMaybes)
import qualified  Data.Serialize as S
import qualified  Data.Set as S
import qualified  Data.Vault.Strict as V
import qualified  Data.Vector as Vc

import Blast.Types
import Blast.Distributed.Types
import Blast.Common.Analyser
import Blast.Master.Analyser

toData :: Maybe a -> Data a
toData (Just a) = Data a
toData Nothing = NoData

getLocalVaultKey :: MExp 'Local a -> LocalKey a
getLocalVaultKey (MLConst _ k _) = k
getLocalVaultKey (MCollect _ k _ _) = k
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

handleRpcError :: Either String t -> t
handleRpcError (Right a) = a
handleRpcError (Left err) = error ("Error in RPC: "++err)

handleRpcErrorM :: (Monad m) => Either String t -> m t
handleRpcErrorM a = return $ handleRpcError a


handleSlaveErrorM :: (Monad m) => SlaveResponse -> m SlaveResponse
handleSlaveErrorM (LsRespError err) = error ("Error in Slave: "++err)
handleSlaveErrorM r = return r

runRemoteOneSlaveStatefull ::(CommandClass s x, MonadIO m) => Int -> MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m ()
runRemoteOneSlaveStatefull slaveId oe@(MRApply n (ExpClosure ce _) e) = do
  s <- getRemote
  let req = LsReqExecute n
  r <- liftIO $ send s slaveId req
  case handleRpcError r of
    LsRespExecute (RcRespCacheMiss CachedFreeVar) -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cached while executing remote on one slave"
        Just (_, Nothing) -> error "local value not cached (BS) while executing remote on one slave"
        Just (_, Just p) -> do
          let ceId = getLocalIndex ce
          let csBs = toData $ getPart slaveId p
          let req' = LsReqCache ceId csBs
          r' <- liftIO $ send s slaveId req'
          sr <- handleRpcErrorM r'
          _ <- handleSlaveErrorM sr
          runRemoteOneSlaveStatefull slaveId oe
    LsRespExecute (RcRespCacheMiss CachedArg) -> do
      runRemoteOneSlaveStatefull slaveId e
      runRemoteOneSlaveStatefull slaveId oe
      -- todo uncache e if should not be cached
    LsRespExecute RcRespOk -> return ()
    LsRespExecute (RcRespError err) -> error ( "remote call: " ++ err)
    LsRespError err -> error ( "remote call: " ++ err)
    _ -> error ( "Should not reach here")

runRemoteOneSlaveStatefull slaveId (MRConst n key _ _) = do
  s <- getRemote
  vault <- getVault
  case V.lookup key vault of
    Just partition -> do
      let bs = toData $ getPart slaveId partition
      let req = LsReqCache n bs
      r <- liftIO $ send s slaveId req
      -- TODO :verify next line.
      sr <- handleRpcErrorM r
      _ <- handleSlaveErrorM sr
      return ()
    Nothing ->  error ("MRConst value not cached"::String)



runRemoteOneSlaveStateless ::(CommandClass s x, MonadIO m) => Int -> [SlaveRequest] -> MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m [SlaveRequest]
runRemoteOneSlaveStateless slaveId requests (MRApply n (ExpClosure ce _) e) = do
  requests' <- runRemoteOneSlaveStateless slaveId requests e
  -- caching value of ce
  vault <- getVault
  let keyce = getLocalVaultKey ce
  let cem = V.lookup keyce vault
  case cem of
    Nothing -> error "local value not cached while executing remote on one slave"
    Just (_, Nothing) -> error "local value not cached (BS) while executing remote on one slave"
    Just (_, Just p) -> do
      let ceId = getLocalIndex ce
      let csBs = toData $ getPart slaveId p
      return (LsReqExecute n : LsReqCache ceId csBs : requests')

runRemoteOneSlaveStateless slaveId requests (MRConst n key _ _) = do
  vault <- getVault
  case V.lookup key vault of
    Just partition -> do
      let bs = toData $ getPart slaveId partition
      return (LsReqCache n bs : requests)
    Nothing ->  error "MRConst value not cached"


fetchOneSlaveResults :: forall a s x m.
  (S.Serialize a, CommandClass s x, MonadIO m)
  => Int -> MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m (Maybe a)
fetchOneSlaveResults slaveId e = do
  s <- getRemote
  let n = getRemoteIndex e
  let req = LsReqFetch n
  rE <- liftIO $ send s slaveId req
  case handleRpcError rE of
    LsRespFetch (Data bs) ->
      case S.decode bs of
        Right v -> return $ Just v
        Left err -> error ("Cannot decode fetched value: " ++ err)
    LsRespFetch NoData -> return Nothing
    LsRespError err -> error ("Cannot fetch results for node: "++ err)
    _ -> error ( "Should not reach here")

fetchResults :: (S.Serialize b, MonadIO m, CommandClass s x) =>
  UnChunkFun b a ->  MExp 'Remote b -> StateT (s x, V.Vault, InfoMap) m a
fetchResults unChunkFun e = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (fetchOneSlaveResults slaveId e) st) slaveIds
  return $ unChunkFun $ catMaybes r


unCacheRemoteOneSlave :: (CommandClass s x, MonadIO m) => Int -> MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m ()
unCacheRemoteOneSlave slaveId e = do
  s <- getRemote
  let req = LsReqUncache (getRemoteIndex e)
  rE <- liftIO $ send s slaveId req
  case handleRpcError rE of
    LsRespVoid -> return ()
    LsRespError err -> error ("Error, uncaching: "++err)
    _ -> error ( "Should not reach here")

unCacheRemote :: (CommandClass s x, MonadIO m) => MExp 'Remote a -> StateT (s x , V.Vault, InfoMap) m ()
unCacheRemote e = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (unCacheRemoteOneSlave slaveId e) st) slaveIds
  return ()

unCacheLocalOneSlave :: (CommandClass s x, MonadIO m) => Int -> MExp 'Local a -> StateT (s x , V.Vault, InfoMap) m ()
unCacheLocalOneSlave slaveId e = do
  s <- getRemote
  let req = LsReqUncache (getLocalIndex e)
  rE <- liftIO $ send s slaveId req
  case handleRpcError rE of
    LsRespVoid -> return ()
    LsRespError err -> error ("Error, uncaching: "++err)
    _ -> error ("Should not reach here")

unCacheLocal :: (CommandClass s x, MonadIO m) => MExp 'Local a -> StateT (s x , V.Vault, InfoMap) m ()
unCacheLocal e = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (unCacheLocalOneSlave slaveId e) st) slaveIds
  return ()

runRemote ::(CommandClass s x, MonadLoggerIO m, S.Serialize b) =>
  UnChunkFun b a -> MExp 'Remote b -> StateT (s x, V.Vault, InfoMap) m a
runRemote unChunkFun e = do
  prepareRunRemote e
  s <- getRemote
  case isStatefullSlave s of
    True -> do
      doRunRemoteStatefull e
      fetchResults unChunkFun e
    False -> doRunRemoteStateless unChunkFun e

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


doRunRemoteStatefull e@(MRConst _ key chunkFun a) = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  let partition = fmap S.encode $ chunkFun nbSlaves a
  let vault' = V.insert key partition vault
  setVault vault'
  st <- get
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runRemoteOneSlaveStatefull slaveId e) st) slaveIds
  return ()

doRunRemoteStateless :: forall a b m s x. (CommandClass s x, MonadLoggerIO m, S.Serialize b)
  => UnChunkFun b a -> MExp 'Remote b -> StateT (s x, V.Vault, InfoMap) m a
doRunRemoteStateless unChunkFun oe@(MRApply n _ _) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  st <- get
  rs <- liftIO $ mapConcurrently (\slaveId -> evalStateT (proc slaveId) st) slaveIds
  return $ unChunkFun $ catMaybes rs
  where
  proc slaveId = do
    requests <- runRemoteOneSlaveStateless slaveId [] oe
    -- fetch the results
    let requests' = L.reverse requests
    s <- getRemote
    let req = LsReqBatch n requests'
    aE <- liftIO $ send s slaveId req
    case handleRpcError aE of
      LsRespBatch (Data bs) ->
        case S.decode bs of
          Right a -> return $ Just (a::b)
          Left err -> error ("Cannot decode value from a batch execution: "++err)
      LsRespBatch NoData -> return Nothing
      LsRespError err -> error ("Batch error: "++err)
      _ -> error ( "Should not reach here")

-- TODO uncomment
doRunRemoteStateless unChunkFun (MRConst _ _ chunkFun a) = do
  return $ unChunkFun [(chunkFun 1 a Vc.! 0)]



-- evaluate all local values (captured in closures)
-- partition and cache MRConst's
prepareRunRemote ::(CommandClass s x, MonadLoggerIO m) => MExp 'Remote a -> StateT (s x, V.Vault, InfoMap) m ()
prepareRunRemote (MRApply _ (ExpClosure ce _) e) = do
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


prepareRunRemote (MRConst _ key chunkFun a) = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let partition = fmap S.encode $ chunkFun nbSlaves a
  let vault' = V.insert key partition vault
  setVault vault'
  return ()

runLocal ::(CommandClass s x, MonadLoggerIO m) => MExp 'Local a -> StateT (s x, V.Vault, InfoMap) m (a, Maybe (Partition BS.ByteString))
runLocal (MLConst _ key a) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just cv -> return cv
    Nothing -> do
      setVault (V.insert key (a, Nothing) vault)
      return (a, Nothing)

runLocal (MCollect n key unChunkFun e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just a -> return a
    Nothing -> do
      a <- runRemote unChunkFun e
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



