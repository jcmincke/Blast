{-# LANGUAGE GADTs #-}


module Blast.Runner.Simple
where



import Debug.Trace

import qualified  Data.Vault.Strict as V
import            Control.Bool (unlessM)
import            Control.Lens (makeLenses, set, view)
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import            Data.IORef
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe
import qualified  Data.Serialize as S
import            GHC.Generics (Generic)

import            Blast.Types
import            Blast.Analyser



runClosure :: Fun a b -> (a -> b)
runClosure (Pure f) = f
runClosure (Closure e f) =
  f r
  where
  r = runLocal e


runRemote :: RemoteExp (Rdd a) -> Rdd a
runRemote (Trans _ _ e cs) =
  Rdd $ mapMaybe f' rdd
  where
  f' = runClosure cs
  (Rdd rdd) = runRemote e
runRemote (ConstRemote _ _ (Rdd x)) = Rdd x
runRemote (Join _ _ a b) = Rdd $ do
  ea <- rdda
  eb <- rddb
  return (ea, eb)
  where
  (Rdd rdda) = runRemote a
  (Rdd rddb) = runRemote b

runLocal :: LocalExp a -> a
runLocal (Collect _ _ e) = runRemote e
runLocal (ConstLocal _ _ a) = a
runLocal (Fold _ _ e f z) =
  L.foldl' f z rdd
  where
  (Rdd rdd) = runRemote e
--runLocal (Apply _ _ f e) = (runLocal f) $ runLocal e
runLocal (FromAppl _ _ e) = runAppl e
runLocal (FMap _ _ f e) = f $ runLocal e

runAppl :: ApplExp a -> a
runAppl (Apply' f e) =
  runAppl f (runLocal e)
runAppl (ConstAppl e) = e


runRemoteD ::(MonadIO m) => InfoMap -> RemoteExp (Rdd a) -> StateT V.Vault m (Rdd a)
runRemoteD m (Trans n _  e (Pure f)) = do
  (Rdd a) <- runRemoteD m e
  let abs = RemoteValue $ S.encode a
  let cbs = RemoteValue$ S.encode ()
  let rcs = getRemoteClosure n m
  vault <- get
  let (r, vault') = rcs vault cbs abs (ResultDescriptor True True)
  put vault'
  case r of
    RemCsResCacheMiss t -> error  ("cache miss: " ++ show t)
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) ->
      case S.decode rbs of
        Right l -> return $ Rdd l
        Left e -> error e

runRemoteD m (Trans n _ e (Closure ce f)) = do
  (Rdd a) <- runRemoteD m e
  c <- runLocalD m ce
  let abs = RemoteValue $ S.encode a
  let cbs = RemoteValue $ S.encode c
  let rcs = getRemoteClosure n m
  vault <- get
  let (r, vault') = rcs vault cbs abs (ResultDescriptor True True)
  put vault'
  case r of
    RemCsResCacheMiss t -> error  ("cache miss: " ++ show t)
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) ->
      case S.decode rbs of
        Right l -> return $ Rdd l
        Left e -> error e

runRemoteD m (ConstRemote _ _ a) = return a

runRemoteD m (Join n _ ae be) = do
  (Rdd as) <- runRemoteD m ae
  (Rdd bs) <- runRemoteD m be
  let r = do
          a <- as
          b <- bs
          return (a, b)
  return $ Rdd r

runLocalD ::(MonadIO m) => InfoMap -> LocalExp a -> StateT V.Vault m a
runLocalD m (Fold n key e f z) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> do liftIO $ print ("hit", n)
                 return v
    Nothing -> do
      liftIO $ print ("hit miss", n)
      (Rdd as) <- runRemoteD m e
      let r = L.foldl' f z as
      vault <- get
      put (V.insert key r vault)
      return r

runLocalD m (ConstLocal _ _ a) = return a

runLocalD m (Collect n key e) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runRemoteD m e
      vault <- get
      put (V.insert key a vault)
      return a

runLocalD m (FromAppl n key e) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runApplD m e
      vault <- get
      put (V.insert key a vault)
      return a

runLocalD m (FMap n key f e) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runLocalD m e
      let r = f a
      vault <- get
      put (V.insert key r vault)
      return r


runApplD ::(MonadIO m) => InfoMap -> ApplExp a -> StateT V.Vault m a
runApplD m (Apply' f a) = do
  f' <- runApplD m f
  a' <- runLocalD m a
  return $ f' a'
runApplD m (ConstAppl a) = return a





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

runRemoteCachingD ::(MonadIO m) => InfoMap -> RemoteExp (Rdd a) -> StateT V.Vault m (Rdd a)
runRemoteCachingD m (Trans n _  e (Pure f)) = do
  a <- runRemoteCachingD m e
  doCacheRemote a e
  let cbs = RemoteValue$ S.encode ()
  let rcs = getRemoteClosure n m
  vault <- get
  let (r, vault') = rcs vault cbs CachedRemoteValue (ResultDescriptor True True)
  put vault'
  case r of
    RemCsResCacheMiss t -> error ("cache miss: " ++ show t)
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) ->
      case S.decode rbs of
        Right l -> return $ Rdd l
        Left e -> error e

runRemoteCachingD m (Trans n _ e (Closure ce f)) = do
  a <- runRemoteCachingD m e
  doCacheRemote a e
  c <- runLocalCachingD m ce
  doCacheLocal c ce
  let rcs = getRemoteClosure n m
  vault <- get
  let (r, vault') = rcs vault CachedRemoteValue CachedRemoteValue (ResultDescriptor True True)
  put vault'
  case r of
    RemCsResCacheMiss t -> error  ("cache miss: " ++ show t)
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) ->
      case S.decode rbs of
        Right l -> return $ Rdd l
        Left e -> error e

runRemoteCachingD m (ConstRemote _ _ a) = return a

runRemoteCachingD m (Join n _ ae be) = do
  a <- runRemoteCachingD m ae
  doCacheRemote a ae
  b <- runRemoteCachingD m be
  doCacheRemote b be
  let rcs = getRemoteClosure n m
  vault <- get
  let (r, vault') = rcs vault CachedRemoteValue CachedRemoteValue (ResultDescriptor True True)
  put vault'
  case r of
    RemCsResCacheMiss t -> error  ("cache miss: " ++ show t)
    ExecRes Nothing -> error "no remote value returned"
    ExecResError e -> error ( "remote call: "++e)
    ExecRes (Just rbs) ->
      case S.decode rbs of
        Right l -> return $ Rdd l
        Left e -> error e

runLocalCachingD ::(MonadIO m) => InfoMap -> LocalExp a -> StateT V.Vault m a
runLocalCachingD m (Fold n key e f z) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> do liftIO $ print ("hit", n)
                 return v
    Nothing -> do
      liftIO $ print ("hit miss", n)
      (Rdd as) <- runRemoteCachingD m e
      let r = L.foldl' f z as
      vault <- get
      put (V.insert key r vault)
      return r

runLocalCachingD m (ConstLocal _ _ a) = return a

runLocalCachingD m (Collect n key e) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runRemoteCachingD m e
      vault <- get
      put (V.insert key a vault)
      return a

runLocalCachingD m (FromAppl n key e) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runApplCachingD m e
      vault <- get
      put (V.insert key a vault)
      return a

runLocalCachingD m (FMap n key f e) = do
  vault <- get
  let cvm = V.lookup key vault
  case cvm of
    Just v -> return v
    Nothing -> do
      a <- runLocalCachingD m e
      let r = f a
      vault <- get
      put (V.insert key r vault)
      return r


runApplCachingD ::(MonadIO m) => InfoMap -> ApplExp a -> StateT V.Vault m a
runApplCachingD m (Apply' f a) = do
  f' <- runApplCachingD m f
  a' <- runLocalCachingD m a
  return $ f' a'
runApplCachingD m (ConstAppl a) = return a










