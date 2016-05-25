{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Analyser
where




import qualified  Data.List as L
import qualified  Data.Vault.Strict as V
import            Control.Bool (unlessM)
import            Control.Lens (set, view)
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Text as T
import            Data.Maybe
import qualified  Data.Serialize as S

import            Blast.Types


refCount :: Int -> InfoMap -> Int
refCount n m =
  case M.lookup n m of
    Just info -> view nbRef info
    Nothing -> error ("Ref count not found for node: " ++ show n)

getRemoteClosure :: Int -> InfoMap -> RemoteClosureImpl
getRemoteClosure n m =
  case M.lookup n m of
    Just info ->  case view remoteClosure info of
                  Just cs -> cs
                  Nothing -> error ("Closure does not exist for node: " ++ show n)
    Nothing -> error ("Closure does not exist for node: " ++ show n)

increaseRef :: Int -> InfoMap -> InfoMap
increaseRef n m =
  case M.lookup n m of
  Just info@(Info old _ _ _ _) -> M.insert n (set nbRef (old+1) info) m
  Nothing -> error $  ("Node " ++ show n ++ " is referenced before being visited")

setRemoteClosure :: Int -> RemoteClosureImpl -> InfoMap -> InfoMap
setRemoteClosure n cs m =
  case M.lookup n m of
  Just info@(Info _ Nothing _ _ _) -> M.insert n (set remoteClosure (Just cs) info) m
  Just (Info _ _ _ _ _) -> m
  Nothing -> error ("Try to set a closure to unvisited node: " ++ show n)

wasVisited :: Int -> InfoMap -> Bool
wasVisited n m = M.member n m

visitExp :: Int -> Cacher -> UnCacher -> IsCached -> InfoMap -> InfoMap
visitExp n cacherFun unCacherFun isCachedFun m =
  case M.lookup n m of
  Just (Info _ _ _ _ _) -> error ("Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (Info 0 Nothing cacherFun unCacherFun isCachedFun) m

increaseRefM :: forall m. MonadLoggerIO m =>
                Int -> StateT InfoMap m ()
increaseRefM n = do
  $(logInfo) $ T.pack ("Referencing node: " ++ show n)
  m <- get
  put (increaseRef n m)

setRemoteClosureM ::  forall m. MonadLoggerIO m =>
                      Int -> RemoteClosureImpl -> StateT InfoMap m ()
setRemoteClosureM n cs = do
  m <- get
  put (setRemoteClosure n cs m)

wasVisitedM ::  forall m. Monad m =>
                Int -> StateT InfoMap m Bool
wasVisitedM n = do
  m <- get
  return $ wasVisited n m

visitExpM ::  forall m. MonadLoggerIO m =>
              Int -> Cacher -> UnCacher -> IsCached -> StateT InfoMap m ()
visitExpM n cacherFun unCacherFun isCachedFun = do
  $(logInfo) $ T.pack  ("Visiting node: " ++ show n)
  m <- get
  put $ visitExp n cacherFun unCacherFun isCachedFun m




mkRemoteClosure :: forall a b m . (MonadLoggerIO m , S.Serialize b, S.Serialize a) =>
  V.Key a -> V.Key b -> ExpClosure a b -> StateT (M.Map Int Info) m RemoteClosureImpl
mkRemoteClosure keya keyb (ExpClosure e f) = do
  analyseLocal e
  let keyc = getLocalVaultKey e
  return $ wrapClosure keyc keya keyb f



mkPreparedFoldClosure :: forall a r m . (MonadLoggerIO m , S.Serialize r, S.Serialize a) =>
  V.Key (Rdd a) -> V.Key (Rdd r) -> PreparedFoldClosure a r -> StateT (M.Map Int Info) m RemoteClosureImpl
mkPreparedFoldClosure keya keyb (PreparedFoldClosure e f) = do
  analyseLocal e
  let keyc = getLocalVaultKey e
  return $ wrapPreparedFoldClosure keyc keya keyb f


wrapPreparedFoldClosure :: forall a r c . (S.Serialize a, S.Serialize r, S.Serialize c) =>
            V.Key (c, r) -> V.Key (Rdd a) -> V.Key (Rdd r) -> (c -> r -> a -> r) -> RemoteClosureImpl
wrapPreparedFoldClosure keyc keya keyb f =
    proc
    where
    proc vault cfv a (ResultDescriptor shouldReturn shouldCache) =
      either (\l -> (l, vault)) id r
      where
      r = do
        (c, z) <- getVal CachedFreeVar vault keyc cfv
        (Rdd al) <- getVal CachedArg vault keya a
        let brdd = Rdd $ [L.foldl' (f c) z al]
        let vault' = if shouldCache then V.insert keyb brdd vault else vault
        let bbsM = if shouldReturn then Just $ S.encode brdd else Nothing
        return (ExecRes bbsM, vault')


wrapClosure :: forall a b c . (S.Serialize a, S.Serialize b, S.Serialize c) =>
            V.Key c -> V.Key a -> V.Key b -> (c -> a -> b) -> RemoteClosureImpl
wrapClosure keyc keya keyb f =
    proc
    where
    proc vault cfv a (ResultDescriptor shouldReturn shouldCache) =
      either (\l -> (l, vault)) id r
      where
      r = do
        c <- getVal CachedFreeVar vault keyc cfv
        av <- getVal CachedArg vault keya a
        let brdd = (f c) av
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
getRemoteIndex (RMap i _ _ _) = i
getRemoteIndex (RConst i _ _) = i

getRemoteVaultKey :: RemoteExp a -> V.Key a
getRemoteVaultKey (RMap _ k _ _) = k
getRemoteVaultKey (RConst _ k _) = k

getLocalIndex :: LocalExp a -> Int
getLocalIndex (LConst i _ _) = i
getLocalIndex (Collect i _ _) = i
getLocalIndex (FromAppl i _ _) = i
getLocalIndex (LMap i _ _ _) = i

getLocalVaultKey :: LocalExp a -> V.Key a
getLocalVaultKey (LConst _ k _) = k
getLocalVaultKey (Collect _ k _) = k
getLocalVaultKey (FromAppl _ k _) = k
getLocalVaultKey (LMap _ k _ _) = k


makeCacher :: (S.Serialize a) => V.Key a -> BS.ByteString -> V.Vault -> V.Vault
makeCacher key bs vault =
  case S.decode bs of
  Left e -> error $ ("Cannot deserialize value: " ++ e)
  Right a -> V.insert key a vault

makeUnCacher :: V.Key a -> V.Vault -> V.Vault
makeUnCacher key vault = V.delete key vault

makeIsCached :: V.Key a -> V.Vault -> Bool
makeIsCached key vault =
  case V.lookup key vault of
  Just _ -> True
  Nothing -> False

analyseRemote :: (MonadLoggerIO m) => RemoteExp a -> StateT InfoMap m ()
analyseRemote (RMap n keyb a cs@(ExpClosure ce _)) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    increaseRefM (getRemoteIndex a)
    analyseLocal ce
    increaseRefM (getLocalIndex ce)
    $(logInfo) $ T.pack ("create closure for RMap node " ++ show n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitExpM n (makeCacher keyb) (makeUnCacher keyb) (makeIsCached keyb)
    setRemoteClosureM n rcs


analyseRemote (RConst n key _) =
  unlessM (wasVisitedM n) $ visitExpM n (makeCacher key) (makeUnCacher key) (makeIsCached key)


analyseLocal :: (MonadLoggerIO m) => LocalExp a -> StateT InfoMap m ()

analyseLocal(LConst n key _) =
  unlessM (wasVisitedM n) $ visitExpM n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal (Collect n key e) =
  unlessM (wasVisitedM n) $ do
    analyseRemote e
    increaseRefM (getRemoteIndex e)
    visitExpM n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal (FromAppl n key e) =
  unlessM (wasVisitedM n) $ do
    analyseAppl e
    visitExpM n (makeCacher key) (makeUnCacher key) (makeIsCached key)

analyseLocal (LMap n key e _) =
  unlessM (wasVisitedM n) $ do
    analyseLocal e
    visitExpM n (makeCacher key) (makeUnCacher key) (makeIsCached key)


analyseAppl :: (MonadLoggerIO m) => ApplyExp a -> StateT InfoMap m ()
analyseAppl (Apply f e) = do
  analyseAppl f
  analyseLocal e

analyseAppl (ConstApply _) = return ()

