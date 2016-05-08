{-# LANGUAGE GADTs #-}
{-# LANGUAGE RecordWildCards #-}


module Blast.Remote
where

import            Control.Concurrent
import            Control.Concurrent.Async
import            Control.Concurrent.Chan
import            Control.Monad
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State

import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Proxy
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V


import Blast.Types
import Blast.Analyser


{-

data RemoteValue a =
  RemoteValue a
  |CachedRemoteValue

data ResultDescriptor b = ResultDescriptor Bool Bool  -- ^ should be returned + should be cached

data CachedValType = CachedArg | CachedFreeVar
  deriving Show

data RemoteClosureResult b =
  RemCsResCacheMiss CachedValType
  |ExecRes (Maybe b)      -- Nothing when results are not returned
  |ExecResError String    --

type RemoteClosure = V.Vault -> (RemoteValue BS.ByteString) -> (RemoteValue BS.ByteString)
                     -> ResultDescriptor (BS.ByteString) -> (RemoteClosureResult BS.ByteString, V.Vault)

                     -}

type RemoteClosureIndex = Int

class RemoteClass s where
  getNbSlaves :: s -> Int
  status :: s -> Int -> IO Bool
  execute :: (S.Serialize c, S.Serialize a, S.Serialize b) => s -> Int -> RemoteClosureIndex -> (RemoteValue c) -> (RemoteValue a) -> ResultDescriptor b -> IO (RemoteClosureResult b)
  cache :: (S.Serialize a) => s -> Int -> Int -> a -> IO Bool
  uncache :: s -> Int -> Int -> IO Bool
  isCached :: s -> Int -> Int -> IO Bool
  reset :: s -> Int -> IO ()


data RemoteChannels = MkRemoteChannels {
  iocOutChan :: Chan LocalSlaveRequest
  ,iocInChan :: Chan LocalSlaveResponse
}

data SimpleRemote = MkSimpleRemote {
  slaveChannels :: M.Map Int RemoteChannels
}


instance RemoteClass SimpleRemote where
  getNbSlaves (MkSimpleRemote {..}) = M.size slaveChannels
  status (MkSimpleRemote {..}) slaveId = do
    liftIO $ print ("M.! a "++show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    writeChan iocOutChan LsReqStatus
    (LsRespBool b) <- readChan iocInChan
    return b
  execute (MkSimpleRemote {..}) slaveId i c a (ResultDescriptor sr sc) = do
    liftIO $ print ("M.! b "++show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let ec = encodeRemoteValue c
    let ea = encodeRemoteValue a
    let req = LsReqExecute i ec ea (ResultDescriptor sr sc)
    writeChan iocOutChan req
    (LocalSlaveExecuteResult resp) <- readChan iocInChan
    case resp of
      RemCsResCacheMiss t -> return $ RemCsResCacheMiss t
      ExecRes Nothing -> return (ExecRes Nothing)
      ExecRes (Just bs) -> do
        case S.decode bs of
          Left e -> error "decode failed"
          Right r -> return (ExecRes $ Just r)
      ExecResError s -> return (ExecResError s)
  cache (MkSimpleRemote {..}) slaveId i a = do
    liftIO $ print ("M.! c "++show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let abs = S.encode a
    let req = LsReqCache i abs
    writeChan iocOutChan req
    (LsRespBool b) <- readChan iocInChan
    return b
  uncache (MkSimpleRemote {..}) slaveId i = do
    liftIO $ print ("M.! d "++show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqUncache i
    writeChan iocOutChan req
    (LsRespBool b) <- readChan iocInChan
    return b
  isCached (MkSimpleRemote {..}) slaveId i = do
    liftIO $ print ("M.! e "++show slaveId)
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqIsCached i
    writeChan iocOutChan req
    (LsRespBool b) <- readChan iocInChan
    return b
  reset (MkSimpleRemote {..}) slaveId = do
    let (MkRemoteChannels {..}) = slaveChannels M.! slaveId
    let req = LsReqReset
    writeChan iocOutChan req
    LsRespVoid <- readChan iocInChan
    return ()

encodeRemoteValue :: (S.Serialize a) => RemoteValue a -> RemoteValue BS.ByteString
encodeRemoteValue (RemoteValue a) = RemoteValue $ S.encode a
encodeRemoteValue CachedRemoteValue = CachedRemoteValue

createSimpleRemote :: LocalExp a -> Int -> IO SimpleRemote
createSimpleRemote exp nbSlaves = do
  infos <- execStateT (analyseLocal exp) M.empty
  m <- foldM (proc infos) M.empty [0..nbSlaves-1]
  return $ MkSimpleRemote m
  where
  proc infos acc i = do
    ls <- createOneSlave i infos
    let rc = MkRemoteChannels (inChan ls) (outChan ls)
    forkIO $ runSlave ls
    return $ M.insert i rc acc

  createOneSlave slaveId infos = do
    iChan <- newChan
    otChan <- newChan
    return $ MkLocalSlave slaveId infos iChan otChan V.empty


data LocalSlaveRequest =
  LsReqStatus
  |LsReqExecute RemoteClosureIndex (RemoteValue BS.ByteString) (RemoteValue BS.ByteString) (ResultDescriptor BS.ByteString)
  |LsReqCache Int BS.ByteString
  |LsReqUncache Int
  |LsReqIsCached Int
  |LsReqReset


data LocalSlaveExecuteResult =
  LsExecResCacheMiss Int
  |LsExecRes (Maybe BS.ByteString)
  |LsExecResError String


data LocalSlaveResponse =
  LsRespBool Bool
  |LsRespVoid
  |LocalSlaveExecuteResult (RemoteClosureResult BS.ByteString)

data LocalSlave = MkLocalSlave {
  localSlaveId :: Int
  , infos :: M.Map Int Info
  , inChan :: Chan LocalSlaveRequest
  , outChan :: Chan LocalSlaveResponse
  , vault :: V.Vault
  }


runSlave :: LocalSlave -> IO ()
runSlave ls@(MkLocalSlave {..}) = do
  req <- readChan inChan
  putStrLn ("slave " ++ show localSlaveId ++ " read request")
  let (resp, ls') = runCommand ls req
  writeChan outChan resp
  runSlave ls'


runCommand :: LocalSlave -> LocalSlaveRequest -> (LocalSlaveResponse, LocalSlave)
runCommand ls LsReqReset = (LsRespVoid, ls {vault = V.empty})
runCommand ls LsReqStatus = (LsRespBool True, ls)
runCommand ls (LsReqExecute i crv arv rdesc) = do
    case M.lookup i (infos ls) of
      Nothing -> (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ (Just cs) _ _ _) -> let
        (res, vault') = cs (vault ls) crv arv rdesc
        ls' = ls {vault =  vault'}
        in (LocalSlaveExecuteResult res, ls')
      Just (Info _ Nothing _ _ _) -> (LocalSlaveExecuteResult (ExecResError ("closure not found: "++show i)), ls)
runCommand ls (LsReqCache i bs) =
    case M.lookup i (infos ls) of
      Nothing -> (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ _ cacher _ _) -> let
        vault' = cacher bs (vault ls)
        in (LsRespBool True, ls {vault = vault'})
runCommand ls (LsReqUncache i) = do
    case M.lookup i (infos ls) of
      Nothing -> (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ _ _ unCacher _) -> let
        vault' = unCacher (vault ls)
        in (LsRespBool True, ls {vault = vault'})
runCommand ls (LsReqIsCached i) = do
    case M.lookup i (infos ls) of
      Nothing -> (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ _ _ _ isCached) -> let
        b = isCached (vault ls)
        in (LsRespBool b, ls)






-- always cache remote values
doCacheRemote :: (Monad m) => a -> RemoteExp a -> StateT V.Vault m ()
doCacheRemote a e = do
  vault <- get
  let key = getRemoteVaultKey e
  let vault' = V.insert key a vault
  put vault'

doCacheLocal :: (Monad m) => a -> LocalExp a ->  StateT V.Vault m ()
doCacheLocal a e = do
  vault <- get
  let key = getLocalVaultKey e
  let vault' = V.insert key a vault
  put vault'

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

runSimpleRemoteOneSlaveNoRet ::(S.Serialize a, RemoteClass s, MonadIO m) => Int -> InfoMap -> RemoteExp (Rdd a) -> StateT (s, V.Vault) m ()
runSimpleRemoteOneSlaveNoRet slaveId m oe@(Trans n _ e (Pure f)) = do
  let rcs = getRemoteClosure n m
  s <- getRemote
  r <- liftIO $ execute s slaveId n (RemoteValue ()) (createRemoteCachedRemoteValue e) (ResultDescriptor False True :: ResultDescriptor ())
  case r of
    RemCsResCacheMiss CachedFreeVar -> error "cache miss on free var in pure closure"
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just _) -> error "no remote value should be returned"


runSimpleRemoteOneSlaveNoRet slaveId m oe@(Trans n _ e (Closure ce f)) = do
  s <- getRemote
  let rcs = getRemoteClosure n m
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
          liftIO $ cache s slaveId ceId c
          runSimpleRemoteOneSlaveNoRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) -> error "no remote value should be returned"


runSimpleRemoteOneSlaveNoRet slaveId m oe@(Join n _ ae be) = do
  s <- getRemote
  let rcs = getRemoteClosure n m
  r <- liftIO $ execute s slaveId n (createRemoteCachedRemoteValue ae) (createRemoteCachedRemoteValue be) (ResultDescriptor False True :: ResultDescriptor ())
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keya = getRemoteVaultKey ae
      let am = V.lookup keya vault
      case am of
        Nothing -> error "first arg of join not cached while executing remote on one slave"
        Just a -> do
          let aId = getRemoteIndex ae
          liftIO $ cache s slaveId aId a
          runSimpleRemoteOneSlaveNoRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m be
      runSimpleRemoteOneSlaveNoRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> return ()
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) -> error "no remote value should be returned"


runSimpleRemoteOneSlaveNoRet slaveId m (ConstRemote n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let subRdd = partitionRdd nbSlaves a L.!! slaveId
  liftIO $ cache s slaveId n subRdd
  return ()



runSimpleRemoteOneSlaveRet ::(S.Serialize a, RemoteClass s, MonadIO m) => Int -> InfoMap -> RemoteExp (Rdd a) -> StateT (s, V.Vault) m (Rdd a)
runSimpleRemoteOneSlaveRet slaveId m oe@(Trans n _ e (Pure f)) = do
  let rcs = getRemoteClosure n m
  s <- getRemote
  r <- liftIO $ execute s slaveId n (RemoteValue ()) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> error "cache miss on free var in pure closure"
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId m oe@(Trans n _ e (Closure ce f)) = do
  s <- getRemote
  let rcs = getRemoteClosure n m
  r <- liftIO $ execute s slaveId n (createLocalCachedRemoteValue ce) (createRemoteCachedRemoteValue e) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyce = getLocalVaultKey ce
      let cem = V.lookup keyce vault
      case cem of
        Nothing -> error "first arg of join not cached while executing remote on one slave"
        Just c -> do
          let cId = getLocalIndex ce
          liftIO $ cache s slaveId cId c
          runSimpleRemoteOneSlaveRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m e
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just b) -> return b





runSimpleRemoteOneSlaveRet slaveId m oe@(Join n _ ae be) = do
  s <- getRemote
  let rcs = getRemoteClosure n m
  r <- liftIO $ execute s slaveId n (createRemoteCachedRemoteValue ae) (createRemoteCachedRemoteValue be) (ResultDescriptor True True)
  case r of
    RemCsResCacheMiss CachedFreeVar -> do
      vault <- getVault
      let keyae = getRemoteVaultKey ae
      let cem = V.lookup keyae vault
      case cem of
        Nothing -> error "local value not cached while executing remote on one slave"
        Just a -> do
          let aeId = getRemoteIndex ae
          liftIO $ cache s slaveId aeId a
          runSimpleRemoteOneSlaveRet slaveId m oe
    RemCsResCacheMiss CachedArg -> do
      runSimpleRemoteOneSlaveNoRet slaveId m be
      runSimpleRemoteOneSlaveRet slaveId m oe
      -- todo uncache e if should not be cached
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just b) -> return b


runSimpleRemoteOneSlaveRet slaveId m (ConstRemote n _ a) = do
  s <- getRemote
  let nbSlaves = getNbSlaves s
  let subRdd = partitionRdd nbSlaves a L.!! slaveId
  liftIO $ cache s slaveId n subRdd
  return subRdd




runSimpleRemoteRet ::(S.Serialize a, RemoteClass s, MonadIO m) => InfoMap -> RemoteExp (Rdd a) -> StateT (s, V.Vault) m (Rdd a)
runSimpleRemoteRet m oe@(Join n _ ae be) = do
  s <- getRemote
  vault <- getVault
  let aid = getRemoteIndex ae
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  alreadyCacheds <- liftIO $ mapConcurrently (\slaveId -> isCached s slaveId aid) slaveIds
  let alreadyCached = L.all id alreadyCacheds
--  when (not alreadyCached) $ do
  when True $ do
      let keya = getRemoteVaultKey ae
      a <-  case V.lookup keya vault of
              Just v -> return v
              Nothing -> do
                r <- runSimpleRemoteRet m ae -- evaluate ae
                vault <- getVault
                setVault (V.insert keya r vault)
                return r
      -- send to all slaves (could be sent to only those with cache miss)
      liftIO $ mapConcurrently (\slaveId -> cache s slaveId aid a) slaveIds
      return ()
  -- ok ae is available on all slaves
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m oe) (s, vault)) slaveIds
  return $ Rdd $ L.concat $ L.map (\(Rdd x) -> x) r


runSimpleRemoteRet m e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveRet slaveId m e) (s, vault)) slaveIds
  return $ Rdd $ L.concat $ L.map (\(Rdd x) -> x) r


runSimpleRemoteNoRet ::(S.Serialize a, RemoteClass s, MonadIO m) => InfoMap -> RemoteExp (Rdd a) -> StateT (s, V.Vault) m ()

runSimpleRemoteNoRet m oe@(Join n _ ae be) = do
  s <- getRemote
  vault <- getVault
  let aid = getRemoteIndex ae
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  alreadyCacheds <- liftIO $ mapConcurrently (\slaveId -> isCached s slaveId aid) slaveIds
  let alreadyCached = L.all id alreadyCacheds
--  when (not alreadyCached) $ do
  when True $ do
      let keya = getRemoteVaultKey ae
      a <-  case V.lookup keya vault of
              Just v -> return v
              Nothing -> do
                r <- runSimpleRemoteRet m ae -- evaluate ae
                liftIO $ let (Rdd l) = r in print $ ("##################", L.length l)
                vault <- getVault
                setVault (V.insert keya r vault)
                return r
      -- send to all slaves (could be sent to only those with cache miss)
      liftIO $ let (Rdd l) = a in print ("%%%%%%%%%%%%%%%",  L.length l)
      liftIO $ mapConcurrently (\slaveId -> cache s slaveId aid a) slaveIds
      return ()
  -- ok ae is available on all slaves
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveNoRet slaveId m oe) (s, vault)) slaveIds
  return ()

runSimpleRemoteNoRet m e = do
  s <- getRemote
  vault <- getVault
  let nbSlaves = getNbSlaves s
  let slaveIds = [0 .. nbSlaves - 1]
  r <- liftIO $ mapConcurrently (\slaveId -> evalStateT (runSimpleRemoteOneSlaveNoRet slaveId m e) (s, vault)) slaveIds
  return ()




runSimpleLocal ::(S.Serialize a, RemoteClass s, MonadIO m) => InfoMap -> LocalExp a -> StateT (s, V.Vault) m a
runSimpleLocal m (Fold n key e f z) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> do liftIO $ print ("hit", n)
                 return v
    Nothing -> do
      liftIO $ print ("hit miss", n)
      (Rdd as) <- runSimpleRemoteRet m e
      let r = L.foldl' f z as
      vault <- getVault
      setVault (V.insert key r vault)
      return r

runSimpleLocal m (ConstLocal _ key a) = do
      vault <- getVault
      setVault (V.insert key a vault)
      return a

runSimpleLocal m (Collect n key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleRemoteRet m e
      vault <- getVault
      setVault (V.insert key a vault)
      return a

runSimpleLocal m (FromAppl n key e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleAppl m e
      vault <- getVault
      setVault (V.insert key a vault)
      return a

runSimpleLocal m (FMap n key f e) = do
  vault <- getVault
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runSimpleLocal m e
      let r = f a
      vault <- getVault
      setVault (V.insert key r vault)
      return r


runSimpleAppl ::(RemoteClass s, MonadIO m) => InfoMap -> ApplExp a -> StateT (s, V.Vault) m a
runSimpleAppl m (Apply' f a) = do
  f' <- runSimpleAppl m f
  a' <- runSimpleLocal m a
  return $ f' a'
runSimpleAppl m (ConstAppl a) = return a


