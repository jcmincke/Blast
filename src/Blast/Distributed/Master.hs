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
import qualified  Data.Text as T
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

runSimpleRemoteOneSlaveNoRet ::(S.Serialize a, RemoteClass s x, MonadIO m) => Int -> InfoMap -> RemoteExp (Rdd a) -> StateT (s x , V.Vault) m ()
runSimpleRemoteOneSlaveNoRet slaveId m oe@(RMap n _ e (Pure _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (RemoteValue ()) (createRemoteCachedRemoteValue e) (ResultDescriptor False True :: ResultDescriptor ())
  case r of
    RemCsResCacheMiss CachedFreeVar -> error "cache miss on free var in pure closure"
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just _) -> error "no remote value should be returned"


runSimpleRemoteOneSlaveNoRet slaveId m oe@(RMap n _ e (Closure ce _)) = do
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





runSimpleRemoteOneSlaveNoRet slaveId m oe@(RFlatMap n _ e (Pure _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (RemoteValue ()) (createRemoteCachedRemoteValue e) (ResultDescriptor False True :: ResultDescriptor ())
  case r of
    RemCsResCacheMiss CachedFreeVar -> error "cache miss on free var in pure closure"
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just _) -> error "no remote value should be returned"


runSimpleRemoteOneSlaveNoRet slaveId m oe@(RFlatMap n _ e (Closure ce _)) = do
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
      _ <- runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just _) -> error "no remote value should be returned"

runSimpleRemoteOneSlaveNoRet slaveId m oe@(RFold n _ e (PreparedFoldClosure ce _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (createLocalCachedRemoteValue ce) (createRemoteCachedRemoteValue e) (ResultDescriptor False True :: ResultDescriptor ())
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cache while executing remote on one slave 2"
        Just c -> do
          let ceId = getLocalIndex ce
          _ <- liftIO $ cache s slaveId ceId c
          runSimpleRemoteOneSlaveNoRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      _ <- runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just b) -> return b





runSimpleRemoteOneSlaveNoRet slaveId _ (RConst n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let subRdd = partitionRdd nbSlaves a L.!! slaveId
  _ <- liftIO $ cache s slaveId n subRdd
  return ()



runSimpleRemoteOneSlaveRet ::(S.Serialize a, RemoteClass s x, MonadIO m) => Int -> InfoMap -> RemoteExp (Rdd a) -> StateT (s x, V.Vault) m (Rdd a)
runSimpleRemoteOneSlaveRet slaveId m oe@(RMap n _ e (Pure _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (RemoteValue ()) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> error "cache miss on free var in pure closure"
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId m oe@(RMap n _ e (Closure ce _)) = do
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


runSimpleRemoteOneSlaveRet slaveId m oe@(RFlatMap n _ e (Pure _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (RemoteValue ()) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> error "cache miss on free var in pure closure"
    RemCsResCacheMiss CachedArg -> do
      _ <- runSimpleRemoteOneSlaveRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId m oe@(RFlatMap n _ e (Closure ce _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (createLocalCachedRemoteValue ce) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cached while executing remote on one slave 1"
        Just c -> do
          let ceId = getLocalIndex ce
          _ <- liftIO $ cache s slaveId ceId c
          runSimpleRemoteOneSlaveRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      _ <- runSimpleRemoteOneSlaveRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId m oe@(RFold n _ e (PreparedFoldClosure ce _)) = do
  s <- getRemote
  r <- liftIO $ execute s slaveId n (createLocalCachedRemoteValue ce) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "local value not cache while executing remote on one slave 2"
        Just c -> do
          let ceId = getLocalIndex ce
          _ <- liftIO $ cache s slaveId ceId c
          runSimpleRemoteOneSlaveRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      _ <- runSimpleRemoteOneSlaveRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError err -> error ( "remote call: " ++ err)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId _rr (RConst n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let subRdd = partitionRdd nbSlaves a L.!! slaveId
  _ <- liftIO $ cache s slaveId n subRdd
  return subRdd


runSimpleRemoteRet ::(S.Serialize a, RemoteClass s x, MonadLoggerIO m) => InfoMap -> RemoteExp (Rdd a) -> StateT (s x, V.Vault) m (Rdd a)
runSimpleRemoteRet m oe@(RMap _ _ _ (Closure ce _)) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m oe) (s, vault)) slaveIds
  return $ Rdd $ L.concat $ L.map (\(Rdd x) -> x) r

runSimpleRemoteRet m oe@(RFlatMap _ _ _ (Closure ce _)) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m oe) (s, vault)) slaveIds
  return $ Rdd $ L.concat $ L.map (\(Rdd x) -> x) r


runSimpleRemoteRet m oe@(RFold _ _ _ (PreparedFoldClosure ce _)) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m oe) (s, vault)) slaveIds
  return $ Rdd $ L.concat $ L.map (\(Rdd x) -> x) r


runSimpleRemoteRet m e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m e) (s, vault)) slaveIds
  return $ Rdd $ L.concat $ L.map (\(Rdd x) -> x) r


runSimpleRemoteNoRet ::(S.Serialize a, RemoteClass s x, MonadLoggerIO m) => InfoMap -> RemoteExp (Rdd a) -> StateT (s x, V.Vault) m ()

runSimpleRemoteNoRet m oe@(RMap _ _ _ (Closure ce _)) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveNoRet slaveId m oe) (s, vault)) slaveIds
  return ()



runSimpleRemoteNoRet m oe@(RFlatMap _ _ _ (Closure ce _)) = do
  _ <- runSimpleLocal m ce
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  _ <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveNoRet slaveId m oe) (s, vault)) slaveIds
  return ()

runSimpleRemoteNoRet m oe@(RFold _ _ _ (PreparedFoldClosure ce _)) = do
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

--todo specific processing for chunking cst rdd and send them to slaves (could be sequential)



runLocalFun :: (RemoteClass s x, MonadLoggerIO m) => InfoMap -> Fun a b -> StateT (s x, V.Vault) m (a->b)
runLocalFun _ (Pure f) = return f
runLocalFun m (Closure e f) = do
  r <- runSimpleLocal m e
  return $ f r

runRemoteFoldClosure :: (RemoteClass s x, MonadLoggerIO m, S.Serialize r) => InfoMap -> PreparedFoldClosure a r -> StateT (s x, V.Vault) m (r, (r -> a -> r))
runRemoteFoldClosure m (PreparedFoldClosure e f) = do
  (c, z) <- runSimpleLocal m e
  return (z, f c)

runSimpleLocal ::(S.Serialize a, RemoteClass s x, MonadLoggerIO m) => InfoMap -> LocalExp a -> StateT (s x, V.Vault) m a
runSimpleLocal m (LFold n key e f) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      $(logInfo) $ T.pack ("Local cache miss for Fold node " ++ show n)
      (Rdd as) <- runSimpleRemoteRet m e
      (z, f') <-  runRemoteFoldClosure m f
      let r = L.foldl' f' z as
      vault' <- getVault
      setVault (V.insert key r vault')
      return r

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

runSimpleLocal m (FromAppl _ key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleAppl m e
      vault' <- getVault
      setVault (V.insert key a vault')
      return a

runSimpleLocal m (LMap _ key f e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleLocal m e
      f' <-  runLocalFun m f
      let r = f' a
      vault' <- getVault
      setVault (V.insert key r vault')
      return r


runSimpleAppl ::(RemoteClass s x, MonadLoggerIO m) => InfoMap -> ApplyExp a -> StateT (s x, V.Vault) m a
runSimpleAppl m (Apply f a) = do
  f' <- runSimpleAppl m f
  a' <- runSimpleLocal m a
  return $ f' a'
runSimpleAppl _ (ConstApply a) = return a


