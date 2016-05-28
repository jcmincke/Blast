{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Analyser
where

import            Control.Bool (unlessM)
import            Control.Lens (set, view)
import            Control.Monad.Logger
import            Control.Monad.IO.Class
import            Control.Monad.Trans.Either
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V

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
  Just info@(Info old _ _) -> M.insert n (set nbRef (old+1) info) m
  Nothing -> error $  ("Node " ++ show n ++ " is referenced before being visited")

setRemoteClosure :: Int -> RemoteClosureImpl -> InfoMap -> InfoMap
setRemoteClosure n cs m =
  case M.lookup n m of
  Just info@(Info _ Nothing _) -> M.insert n (set remoteClosure (Just cs) info) m
  Just (Info _ _ _) -> m
  Nothing -> error ("Try to set a closure to unvisited node: " ++ show n)

wasVisited :: Int -> InfoMap -> Bool
wasVisited n m = M.member n m

visitExp :: (S.Serialize a) => Int -> V.Key a -> InfoMap -> InfoMap
visitExp n key  m =
  case M.lookup n m of
  Just (Info _ _ _) -> error ("Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (Info 0 Nothing (Just $ makeCacheInfo key)) m

visitExpNoCache :: Int -> InfoMap -> InfoMap
visitExpNoCache n m =
  case M.lookup n m of
  Just (Info _ _ _) -> error ("Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (Info 0 Nothing Nothing) m

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

visitExpM ::  forall a m. (S.Serialize a, MonadLoggerIO m) =>
              Int -> V.Key a  -> StateT InfoMap m ()
visitExpM n key = do
  $(logInfo) $ T.pack  ("Visiting node: " ++ show n)
  m <- get
  put $ visitExp n key m


visitExpNoCacheM :: forall m. (MonadLoggerIO m) =>
  Int ->  StateT InfoMap m ()
visitExpNoCacheM n = do
  $(logInfo) $ T.pack  ("Visiting node (no cache): " ++ show n)
  m <- get
  put $ visitExpNoCache n m



mkRemoteClosure :: forall a b m . (MonadLoggerIO m , S.Serialize b, S.Serialize a) =>
  V.Key a -> V.Key b -> ExpClosure a b -> StateT (M.Map Int Info) m RemoteClosureImpl
mkRemoteClosure keya keyb (ExpClosure e f) = do
  analyseSerializeLocal e
  let keyc = getLocalVaultKey e
  return $ wrapClosure keyc keya keyb f


wrapClosure :: forall a b c . (S.Serialize a, S.Serialize b, S.Serialize c) =>
            V.Key c -> V.Key a -> V.Key b -> (c -> a -> IO b) -> RemoteClosureImpl
wrapClosure keyc keya keyb f =
    proc
    where
    proc vault cfv a (ResultDescriptor shouldReturn shouldCache) = do
      r' <- runEitherT r
      return $ either (\l -> (l, vault)) id r'
      where
      r = do
        c <- getVal CachedFreeVar vault keyc cfv
        av <- getVal CachedArg vault keya a
        brdd <- liftIO $ (f c) av
        let vault' = if shouldCache then V.insert keyb brdd vault else vault
        let bbsM = if shouldReturn then Just $ S.encode brdd else Nothing
        return (ExecRes bbsM, vault')

getVal :: (S.Serialize a, Monad m) =>  CachedValType -> V.Vault -> V.Key a -> RemoteValue BS.ByteString -> EitherT (RemoteClosureResult b) m a
getVal _ _ _ (RemoteValue bs) =
  case S.decode bs of
  Right v -> right v
  Left e -> left $ ExecResError (show e)
getVal cvt vault key CachedRemoteValue =
  case V.lookup key vault of
  Just v -> right v
  Nothing -> left $ RemCsResCacheMiss cvt



getRemoteIndex :: RemoteExp a -> Int
getRemoteIndex (RMap i _ _ _) = i
getRemoteIndex (RConst i _ _) = i

getRemoteVaultKey :: RemoteExp a -> V.Key a
getRemoteVaultKey (RMap _ k _ _) = k
getRemoteVaultKey (RConst _ k _) = k

getLocalIndex :: LocalExp a -> Int
getLocalIndex (LConst i _ _) = i
getLocalIndex (Collect i _ _) = i
getLocalIndex (FMap i _ _ _) = i

getLocalVaultKey :: LocalExp a -> V.Key a
getLocalVaultKey (LConst _ k _) = k
getLocalVaultKey (Collect _ k _) = k
getLocalVaultKey (FMap _ k _ _) = k


makeCacheInfo :: (S.Serialize a) => V.Key a -> CacheInfo
makeCacheInfo key =
  MkCacheInfo (makeCacher key) (makeUnCacher key) (makeIsCached key)
  where
  makeCacher :: (S.Serialize a) => V.Key a -> BS.ByteString -> V.Vault -> V.Vault
  makeCacher k bs vault =
    case S.decode bs of
    Left e -> error $ ("Cannot deserialize value: " ++ e)
    Right a -> V.insert k a vault

  makeUnCacher :: V.Key a -> V.Vault -> V.Vault
  makeUnCacher k vault = V.delete k vault

  makeIsCached :: V.Key a -> V.Vault -> Bool
  makeIsCached k vault =
    case V.lookup k vault of
    Just _ -> True
    Nothing -> False

analyseRemote :: (MonadLoggerIO m) => RemoteExp a -> StateT InfoMap m ()
analyseRemote (RMap n keyb cs@(ExpClosure ce _) a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    increaseRefM (getRemoteIndex a)
    analyseSerializeLocal ce
    increaseRefM (getLocalIndex ce)
    $(logInfo) $ T.pack ("create closure for RMap node " ++ show n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitExpM n keyb
    setRemoteClosureM n rcs


analyseRemote (RConst n key _) =
  unlessM (wasVisitedM n) $ visitExpM n key


analyseSerializeLocal :: (S.Serialize a, MonadLoggerIO m) => LocalExp a -> StateT InfoMap m ()

analyseSerializeLocal(LConst n key _) =
  unlessM (wasVisitedM n) $ visitExpM n key

analyseSerializeLocal (Collect n key e) =
  unlessM (wasVisitedM n) $ do
    analyseRemote e
    increaseRefM (getRemoteIndex e)
    visitExpM n key

analyseSerializeLocal (FMap n key f e) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal e
    visitExpM n key

analyseLocal :: (MonadLoggerIO m) => LocalExp a -> StateT InfoMap m ()

analyseLocal(LConst n _ _) =
  unlessM (wasVisitedM n) $ visitExpNoCacheM n

analyseLocal (Collect n _ e) =
  unlessM (wasVisitedM n) $ do
    analyseRemote e
    increaseRefM (getRemoteIndex e)
    visitExpNoCacheM n

analyseLocal (FMap n _ f e) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal e
    visitExpNoCacheM n

