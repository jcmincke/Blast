{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

module Blast.Analyser
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


refCount :: Int -> InfoMap -> Int
refCount n m =
  case M.lookup n m of
    Just info -> view nbRef info
    Nothing -> error $ show ("ref count not found for "::String, n)

getRemoteClosure :: Int -> InfoMap -> RemoteClosure
getRemoteClosure n m =
  case M.lookup n m of
    Just info ->  case view remoteClosure info of
                  Just cs -> cs
                  Nothing -> error "remote closure does not exist"
    Nothing -> error $ show ("closure not found for "::String, n)



increaseRef :: Int -> InfoMap -> InfoMap
increaseRef n m =
  case M.lookup n m of
  Just info@(Info old _) -> M.insert n (set nbRef (old+1) info) m
  Nothing -> error $ show ("referenced before being visited"::String, n)

setRemoteClosure :: Int -> RemoteClosure -> InfoMap -> InfoMap
setRemoteClosure n cs m =
  case M.lookup n m of
  Just info@(Info _ Nothing) -> M.insert n (set remoteClosure (Just cs) info) m
  Just (Info _ _) -> m
  Nothing -> error "set closure before being visited"

wasVisited :: Int -> InfoMap -> Bool
wasVisited n m = M.member n m

visitExp :: Int -> InfoMap -> InfoMap
visitExp n m =
  case M.lookup n m of
  Just (Info c cs) -> M.insert n (Info c cs) m
  Nothing -> M.insert n (Info 0 Nothing) m

increaseRef' n = do
  liftIO $ print ("reference: ", n)
  m <- get
  put (increaseRef n m)

setRemoteClosure' n cs = do
  m <- get
  put (setRemoteClosure n cs m)

wasVisited' n = do
  m <- get
  return $ wasVisited n m

visitExp' n = do
  liftIO $ print ("visit: ", n)
  m <- get
  put $ visitExp n m




mkRemoteClosure :: forall a b m . (MonadIO m , S.Serialize b, S.Serialize a) =>
  V.Key (Rdd a) -> V.Key (Rdd b) -> Fun a (Maybe b) -> StateT (M.Map Int Info) m RemoteClosure
mkRemoteClosure keya keyb cs = do
  case cs of
    (Pure f) -> return $ wrapPure keya keyb f
    (Closure e f) -> do
      analyseLocal e
      let keyc = getLocalVaultKey e
      return $ wrapClosure keyc keya keyb f


wrapPure :: forall a b. (S.Serialize b, S.Serialize a) =>
            V.Key (Rdd a) -> V.Key (Rdd b) -> (a -> Maybe b) -> RemoteClosure
wrapPure keya keyb f =
    proc
    where
    proc vault _ a (ResultDescriptor shouldReturn shouldCache) =
      either (\l -> (l, vault)) id r
      where
      r = do
        (Rdd al) <- getVal CachedArg vault keya a
        let brdd = Rdd $ mapMaybe f al
        let vault' = if shouldCache then V.insert keyb brdd vault else vault
        let bbsM = if shouldReturn then Just $ S.encode brdd else Nothing
        return (ExecRes bbsM, vault')




wrapClosure :: forall a b c . (S.Serialize a, S.Serialize b, S.Serialize c) =>
            V.Key c -> V.Key (Rdd a) -> V.Key (Rdd b) -> (c -> a -> Maybe b) -> RemoteClosure
wrapClosure keyc keya keyb f =
    proc
    where
    proc vault cfv a (ResultDescriptor shouldReturn shouldCache) =
      either (\l -> (l, vault)) id r
      where
      r = do
        c <- getVal CachedFreeVar vault keyc cfv
        (Rdd al) <- getVal CachedArg vault keya a
        let brdd = Rdd $ mapMaybe (f c) al
        let vault' = if shouldCache then V.insert keyb brdd vault else vault
        let bbsM = if shouldReturn then Just $ S.encode brdd else Nothing
        return (ExecRes bbsM, vault')

getVal :: (S.Serialize a) =>  CachedValType -> V.Vault -> V.Key a -> RemoteValue BS.ByteString -> Either (RemoteClosureResult b) a
getVal _ _ _ (RemoteValue bs) =
  case S.decode bs of
  Right v -> Right v
  Left e -> Left $ ExecResError (show e)
getVal cvt vault key CachedRemoteValue =
  case V.lookup key vault of
  Just v -> Right v
  Nothing -> Left $ RemCsResCacheMiss cvt



getRemoteIndex :: RemoteExp a -> Int
getRemoteIndex (Trans i _ _ _) = i
getRemoteIndex (ConstRemote i _ _) = i
getRemoteIndex (Join i _ _ _) = i

getRemoteVaultKey :: RemoteExp a -> V.Key a
getRemoteVaultKey (Trans _ k _ _) = k
getRemoteVaultKey (ConstRemote _ k _) = k
getRemoteVaultKey (Join _ k _ _) = k


getLocalIndex :: LocalExp a -> Int
getLocalIndex (Fold i _ _ _ _) = i
getLocalIndex (ConstLocal i _ _) = i
getLocalIndex (Collect i _ _) = i
getLocalIndex (Apply i _ _ _) = i
getLocalIndex (FMap i _ _ _) = i

getLocalVaultKey :: LocalExp a -> V.Key a
getLocalVaultKey (Fold _ k _ _ _) = k
getLocalVaultKey (ConstLocal _ k _) = k
getLocalVaultKey (Collect _ k _) = k
getLocalVaultKey (Apply _ k _ _) = k
getLocalVaultKey (FMap _ k _ _) = k



analyseRemote :: (MonadIO m) => RemoteExp a -> StateT InfoMap m ()
analyseRemote (Trans n keyb a cs@(Pure _)) =
  unlessM (wasVisited' n) $ do
    analyseRemote a
    increaseRef' (getRemoteIndex a)
    liftIO $ print ("create pure remote closure"::String, n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitExp' n
    setRemoteClosure' n rcs

analyseRemote (Trans n keyb a cs@(Closure ce _)) =
  unlessM (wasVisited' n) $ do
    analyseRemote a
    increaseRef' (getRemoteIndex a)
    analyseLocal ce
    increaseRef' (getLocalIndex ce)
    liftIO $ print ("create closure remote closure"::String, n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitExp' n
    setRemoteClosure' n rcs

analyseRemote e@(ConstRemote n _ _) = visitExp' n

analyseRemote e@(Join n _ a b) =
  unlessM (wasVisited' n) $ do
    analyseRemote a
    analyseRemote b
    increaseRef' (getRemoteIndex a)
    increaseRef' (getRemoteIndex b)
    visitExp' n

analyseLocal :: (MonadIO m) => LocalExp a -> StateT InfoMap m ()
analyseLocal (Fold n lc e f z) =
  unlessM (wasVisited' n) $ do
    analyseRemote e
    increaseRef' (getRemoteIndex e)
    visitExp' n

analyseLocal(ConstLocal n _ _) = visitExp' n

analyseLocal (Collect n lc e) =
  unlessM (wasVisited' n) $ do
    analyseRemote e
    increaseRef' (getRemoteIndex e)
    visitExp' n

analyseLocal (Apply n lc f e) =
  unlessM (wasVisited' n) $ do
    analyseLocal f
    analyseLocal e
    visitExp' n

analyseLocal (FMap n lc f e) =
  unlessM (wasVisited' n) $ do
    analyseLocal e
    visitExp' n

