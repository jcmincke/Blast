{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Analyser
where

import Debug.Trace
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
    Just (Info _ (NtRMap (MkRMapInfo cs _ _)))   -> cs
    Nothing -> error ("Closure does not exist for node: " ++ show n)

increaseRef :: Int -> InfoMap -> InfoMap
increaseRef n m =
  case M.lookup n m of
  Just info@(Info old _) -> M.insert n (set nbRef (old+1) info) m
  Nothing -> error $  ("Node " ++ show n ++ " is referenced before being visited")


makeLocalCacheInfo :: (S.Serialize a) => LocalKey a -> LExpInfo
makeLocalCacheInfo key =
  MkLExpInfo (makeCacher key) (makeUnCacher key)
  where
  makeCacher :: (S.Serialize a) => LocalKey a -> BS.ByteString -> V.Vault -> V.Vault
  makeCacher k bs vault =
    case S.decode bs of
    Left e -> error $ ("Cannot deserialize value: " ++ e)
    Right a -> V.insert k (a, Nothing) vault

  makeUnCacher :: LocalKey a -> V.Vault -> V.Vault
  makeUnCacher k vault = V.delete k vault

  makeIsCached :: LocalKey a -> V.Vault -> Bool
  makeIsCached k vault =
    case V.lookup k vault of
    Just _ -> True
    Nothing -> False


wasVisited :: Int -> InfoMap -> Bool
wasVisited n m = M.member n m

visitRConstExp :: (S.Serialize a) => Int -> V.Key a -> InfoMap -> InfoMap
visitRConstExp n key  m =
  case M.lookup n m of
  Just (Info _ _) -> error ("RConst Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (Info 0 (NtRConst (MkRConstInfo (makeCacher key) (makeUnCacher key) Nothing))) m
  where
  makeCacher :: (S.Serialize a) => V.Key a -> BS.ByteString -> V.Vault -> V.Vault
  makeCacher k bs vault =
    case S.decode bs of
    Left e -> error $ ("Cannot deserialize value: " ++ e)
    Right a -> V.insert k a vault

  makeUnCacher :: V.Key a -> V.Vault -> V.Vault
  makeUnCacher k vault = V.delete k vault


visitRConstExpM ::  forall a m. (MonadLoggerIO m, S.Serialize a) =>
              Int -> V.Key a  -> StateT InfoMap m ()
visitRConstExpM n key = do
  $(logInfo) $ T.pack  ("Visiting RConst node: " ++ show n)
  m <- get
  put $ visitRConstExp n key m


visitRMapExp :: Int -> V.Key a -> RemoteClosureImpl -> InfoMap -> InfoMap
visitRMapExp n key cs m =
  case M.lookup n m of
  Just (Info _ _) -> error ("RMap Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (Info 0 (NtRMap (MkRMapInfo cs (makeUnCacher key) Nothing))) m
  where

  makeUnCacher :: V.Key a -> V.Vault -> V.Vault
  makeUnCacher k vault = V.delete k vault


visitRMapExpM ::  forall a m. (MonadLoggerIO m) =>
              Int -> V.Key a  -> RemoteClosureImpl -> StateT InfoMap m ()
visitRMapExpM n key cs = do
  $(logInfo) $ T.pack  ("Visiting RMap node: " ++ show n)
  m <- get
  put $ visitRMapExp n key cs m




visitLocalExp :: Int -> InfoMap -> InfoMap
visitLocalExp n m =
  case M.lookup n m of
  Just (Info _ _ ) -> error ("Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (Info 0 NtLExpNoCache) m



visitLocalExpM :: forall a m. (MonadLoggerIO m) => Int -> StateT InfoMap m ()
visitLocalExpM n = do
  $(logInfo) $ T.pack  ("Visiting local exp node: " ++ show n)
  m <- get
  put $ visitLocalExp n m


addLocalExpCache :: (S.Serialize a) => Int -> LocalKey a -> InfoMap -> InfoMap
addLocalExpCache n key m =
  case M.lookup n m of
  Just (Info c NtLExpNoCache) -> M.insert n (Info c (NtLExp (MkLExpInfo (makeCacher key) (makeUnCacher key)))) m
  Nothing -> M.insert n (Info 0 (NtLExp (MkLExpInfo (makeCacher key) (makeUnCacher key)))) m
  Just (Info _ (NtLExp _)) -> m
  _ ->  error ("Node " ++ show n ++ " cannot add local exp cache")
  where
  makeCacher :: (S.Serialize a) => LocalKey a -> BS.ByteString -> V.Vault -> V.Vault
  makeCacher k bs vault =
    case S.decode bs of
    Left e -> error $ ("Cannot deserialize value: " ++ e)
    Right a -> V.insert k (a, Nothing) vault

  makeUnCacher :: LocalKey a -> V.Vault -> V.Vault
  makeUnCacher k vault = V.delete k vault

addLocalExpCacheM :: forall a m. (MonadLoggerIO m, S.Serialize a) =>
  LocalExp a -> StateT InfoMap m ()
addLocalExpCacheM e = do
  let n = getLocalIndex e
  $(logInfo) $ T.pack  ("Adding cache to local exp node: " ++ show n)
  let key = getLocalVaultKey e
  m <- get
  put $ addLocalExpCache n key m

addRemoteExpCacheReader :: (S.Serialize a) => Int -> V.Key a -> InfoMap -> InfoMap
addRemoteExpCacheReader n key m =
  case M.lookup n m of
  Just (Info _ (NtRMap (MkRMapInfo _ _ (Just _)))) -> m
  Just (Info c (NtRMap (MkRMapInfo cs uncacher Nothing))) ->
    M.insert n (Info c (NtRMap (MkRMapInfo cs uncacher (Just $ makeCacheReader key)))) m
  Just (Info _ (NtRConst (MkRConstInfo _ _ (Just _)))) -> m
  Just (Info c (NtRConst (MkRConstInfo cacher uncacher Nothing))) ->
    trace ("oui") $ M.insert n (Info c (NtRConst (MkRConstInfo cacher uncacher (Just $ makeCacheReader key)))) m
  _ ->  error ("Node " ++ show n ++ " cannot add remote exp cache reader")
  where
  makeCacheReader :: (S.Serialize a) => V.Key a -> V.Vault -> Maybe BS.ByteString
  makeCacheReader key vault =
    case V.lookup key vault of
      Nothing -> Nothing
      Just b -> Just $ S.encode b


addRemoteExpCacheReaderM ::
  forall a m. (MonadLoggerIO m, S.Serialize a)
  => RemoteExp a -> StateT InfoMap m ()
addRemoteExpCacheReaderM e = do
  let n = getRemoteIndex e
  $(logInfo) $ T.pack  ("Adding cache reader to remote exp node: " ++ show n)
  let key = getRemoteVaultKey e
  m <- get
  put $ addRemoteExpCacheReader n key m

increaseRefM :: forall m. MonadLoggerIO m =>
                Int -> StateT InfoMap m ()
increaseRefM n = do
  $(logInfo) $ T.pack ("Referencing node: " ++ show n)
  m <- get
  put (increaseRef n m)

wasVisitedM ::  forall m. Monad m =>
                Int -> StateT InfoMap m Bool
wasVisitedM n = do
  m <- get
  return $ wasVisited n m




mkRemoteClosure :: forall a b m . (MonadLoggerIO m) =>
  V.Key a -> V.Key b -> ExpClosure a b -> StateT (M.Map Int Info) m RemoteClosureImpl
mkRemoteClosure keya keyb (ExpClosure e f) = do
  analyseLocal e
  addLocalExpCacheM e
  let keyc = getLocalVaultKey e
  return $ wrapClosure keyc keya keyb f


wrapClosure :: forall a b c .
            LocalKey c -> V.Key a -> V.Key b -> (c -> a -> IO b) -> RemoteClosureImpl
wrapClosure keyc keya keyb f =
    proc
    where
    proc vault = do
      r' <- runEitherT r
      return $ either (\l -> (l, vault)) id r'
      where
      r = do
        c <- getLocalVal CachedFreeVar vault keyc
        av <- getVal CachedArg vault keya
        brdd <- liftIO $ (f c) av
        let vault' = V.insert keyb brdd vault
        return (ExecRes, vault')

getVal :: (Monad m) =>  CachedValType -> V.Vault -> V.Key a -> EitherT RemoteClosureResult m a
getVal cvt vault key =
  case V.lookup key vault of
  Just v -> right v
  Nothing -> left $ RemCsResCacheMiss cvt

getLocalVal :: (Monad m) =>  CachedValType -> V.Vault -> LocalKey a -> EitherT RemoteClosureResult m a
getLocalVal cvt vault key  =
  case V.lookup key vault of
  Just (v, _) -> right v
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

getLocalVaultKey :: LocalExp a -> LocalKey a
getLocalVaultKey (LConst _ k _) = k
getLocalVaultKey (Collect _ k _) = k
getLocalVaultKey (FMap _ k _ _) = k


analyseRemote :: (MonadLoggerIO m) => RemoteExp a -> StateT InfoMap m ()
analyseRemote (RMap n keyb cs@(ExpClosure ce _) a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    increaseRefM (getRemoteIndex a)
    analyseLocal ce
    addLocalExpCacheM ce
    increaseRefM (getLocalIndex ce)
    $(logInfo) $ T.pack ("create closure for RMap node " ++ show n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitRMapExpM n keyb rcs


analyseRemote (RConst n key _) =
  unlessM (wasVisitedM n) $ visitRConstExpM n key


analyseLocal :: (MonadLoggerIO m) => LocalExp a -> StateT InfoMap m ()

analyseLocal(LConst n _ _) =
  unlessM (wasVisitedM n) $ visitLocalExpM n

analyseLocal (Collect n _ e) =
  unlessM (wasVisitedM n) $ do
    analyseRemote e
    addRemoteExpCacheReaderM e
    increaseRefM (getRemoteIndex e)
    visitLocalExpM n

analyseLocal (FMap n _ f e) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal e
    visitLocalExpM n

