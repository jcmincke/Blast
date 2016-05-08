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
                  Nothing -> error ("remote closure does not exist for " ++ show n)
    Nothing -> error $ show ("closure not found for "::String, n)

increaseRef :: Int -> InfoMap -> InfoMap
increaseRef n m =
  case M.lookup n m of
  Just info@(Info old _ _ _ _) -> M.insert n (set nbRef (old+1) info) m
  Nothing -> error $ show ("referenced before being visited"::String, n)

setRemoteClosure :: Int -> RemoteClosure -> InfoMap -> InfoMap
setRemoteClosure n cs m =
  case M.lookup n m of
  Just info@(Info _ Nothing _ _ _) -> M.insert n (set remoteClosure (Just cs) info) m
  Just (Info _ _ _ _ _) -> m
  Nothing -> error "set closure before being visited"

wasVisited :: Int -> InfoMap -> Bool
wasVisited n m = M.member n m

visitExp :: Int -> Cacher -> UnCacher -> IsCached -> InfoMap -> InfoMap
visitExp n cacher unCacher isCached m =
  case M.lookup n m of
  Just (Info n cs _ _ _) -> error ("exp already visited: " ++ show n)
  Nothing -> M.insert n (Info 0 Nothing cacher unCacher isCached) m

increaseRef' n = do
  liftIO $ print ("reference: ", n)
  m <- get
  put (increaseRef n m)

setRemoteClosure' n cs = do
  m <- get
  liftIO $ print ("set closure for ", n)
  put (setRemoteClosure n cs m)

wasVisited' n = do
  m <- get
  return $ wasVisited n m

visitExp' n cacher unCacher isCached = do
  liftIO $ print ("visit: ", n)
  m <- get
  put $ visitExp n cacher unCacher isCached m




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




wrapJoinClosure :: forall a b . (S.Serialize a, S.Serialize b) =>
                   V.Key (Rdd a) -> V.Key (Rdd b) ->  V.Key (Rdd (a, b)) -> RemoteClosure
wrapJoinClosure keya keyb keyab =
    proc
    where
    proc vault abs bbs (ResultDescriptor shouldReturn shouldCache) =
      either (\l -> (l, vault)) id r
      where
      r = do
        (Rdd al) <- getVal CachedArg vault keya abs
        (Rdd bl) <- getVal CachedArg vault keyb bbs
        let r = Rdd $ do  a <- al
                          b <- bl
                          return (a, b)
        let vault' = if shouldCache then V.insert keyab r vault else vault
        let rbsM = if shouldReturn then Just $ S.encode r else Nothing
        return (ExecRes rbsM, vault')

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
--getLocalIndex (Apply i _ _ _) = i
getLocalIndex (FromAppl i _ _) = i
getLocalIndex (FMap i _ _ _) = i

getLocalVaultKey :: LocalExp a -> V.Key a
getLocalVaultKey (Fold _ k _ _ _) = k
getLocalVaultKey (ConstLocal _ k _) = k
getLocalVaultKey (Collect _ k _) = k
--getLocalVaultKey (Apply _ k _ _) = k
getLocalVaultKey (FromAppl _ k _) = k
getLocalVaultKey (FMap _ k _ _) = k


makeCacher :: (S.Serialize a) => V.Key a -> BS.ByteString -> V.Vault -> V.Vault
makeCacher key bs vault =
  case S.decode bs of
  Left e -> error $ show e
  Right a -> V.insert key a vault

makeUnCacher :: V.Key a -> V.Vault -> V.Vault
makeUnCacher key vault = V.delete key vault

makeIsCached :: V.Key a -> V.Vault -> Bool
makeIsCached key vault =
  case V.lookup key vault of
  Just _ -> True
  Nothing -> False

analyseRemote :: (MonadIO m) => RemoteExp a -> StateT InfoMap m ()
analyseRemote (Trans n keyb a cs@(Pure _)) =
  unlessM (wasVisited' n) $ do
    analyseRemote a
    increaseRef' (getRemoteIndex a)
    liftIO $ print ("create pure remote closure"::String, n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitExp' n (makeCacher keyb) (makeUnCacher keyb) (makeIsCached keyb)
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
    visitExp' n (makeCacher keyb) (makeUnCacher keyb) (makeIsCached keyb)
    setRemoteClosure' n rcs

analyseRemote e@(ConstRemote n key _) =
  unlessM (wasVisited' n) $ visitExp' n (makeCacher key) (makeUnCacher key) (makeIsCached key)


analyseRemote e@(Join n keyab a b) =
  unlessM (wasVisited' n) $ do
    analyseRemote a
    analyseRemote b
    increaseRef' (getRemoteIndex a)
    increaseRef' (getRemoteIndex b)
    visitExp' n (makeCacher keyab) (makeUnCacher keyab) (makeIsCached keyab)
    let keya = getRemoteVaultKey a
    let keyb = getRemoteVaultKey b
    let rcs = wrapJoinClosure keya keyb keyab
    setRemoteClosure' n rcs

analyseLocal :: (MonadIO m) => LocalExp a -> StateT InfoMap m ()
analyseLocal (Fold n key e f z) =
  unlessM (wasVisited' n) $ do
    analyseRemote e
    increaseRef' (getRemoteIndex e)
    visitExp' n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal(ConstLocal n key _) =
  unlessM (wasVisited' n) $ visitExp' n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal (Collect n key e) =
  unlessM (wasVisited' n) $ do
    analyseRemote e
    increaseRef' (getRemoteIndex e)
    visitExp' n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal (FromAppl n key e) =
  unlessM (wasVisited' n) $ do
    analyseAppl e
    visitExp' n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal (FMap n key f e) =
  unlessM (wasVisited' n) $ do
    analyseLocal e
    visitExp' n (makeCacher key) (makeUnCacher key) (makeIsCached key)


analyseAppl :: (MonadIO m) => ApplExp a -> StateT InfoMap m ()
analyseAppl (Apply' f e) = do
  analyseAppl f
  analyseLocal e

analyseAppl (ConstAppl _) = return ()

