{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Slave.Analyser
(
  SExp (..)
  , NodeTypeInfo (..)
  , LExpInfo (..)
  , RConstInfo (..)
  , RMapInfo (..)
  , InfoMap
  , analyseLocal
)
where

--import Debug.Trace
import            Control.Bool (unlessM)
import            Control.Monad.Logger
import            Control.Monad.IO.Class
import            Control.Monad.Trans.Either
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Set as S
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V

import            Blast.Types
import            Blast.Common.Analyser



data SExp (k::Kind) a where
  SRApply :: Int -> V.Key (Data b) -> ExpClosure SExp a b -> SExp 'Remote a -> SExp 'Remote b
  SRConst ::  (S.Serialize b) => Int -> V.Key (Data b) -> a -> SExp 'Remote b
  SLConst :: Int -> V.Key a -> a -> SExp 'Local a
  SCollect :: (S.Serialize b) => Int -> V.Key a -> SExp 'Remote b -> SExp 'Local a
  SLApply :: Int -> V.Key b -> SExp 'Local (a -> b) -> SExp 'Local a -> SExp 'Local b



instance (MonadLoggerIO m) => Builder m SExp where
  makeRApply i f a = do
    k <- liftIO V.newKey
    return $ SRApply i k f a
  makeRConst i _ a = do
    k <- liftIO V.newKey
    return $ SRConst i k a
  makeLConst i a = do
    k <- liftIO V.newKey
    return $ SLConst i k a
  makeCollect i _ a = do
    k <- liftIO V.newKey
    return $ SCollect i k a
  makeLApply i f a = do
    k <- liftIO V.newKey
    return $ SLApply i k f a
  fuse refMap n e = fuseRemote refMap n e

instance Indexable SExp where
  getIndex (SRApply n _ _ _) = n
  getIndex (SRConst n _ _) = n
  getIndex (SLConst n _ _) = n
  getIndex (SCollect n _ _) = n
  getIndex (SLApply n _ _ _) = n



type RemoteCacher = Data BS.ByteString -> V.Vault -> V.Vault
type LocalCacher = BS.ByteString -> V.Vault -> V.Vault

type RemoteCacheReader = V.Vault -> Maybe (Data BS.ByteString)

type UnCacher = V.Vault -> V.Vault

data NodeTypeInfo =
  NtRMap RMapInfo
  |NtRConst RConstInfo
  |NtLExp LExpInfo
  |NtLExpNoCache

data RMapInfo = MkRMapInfo {
  _rmRemoteClosure :: RemoteClosureImpl
  , _rmUnCacher :: UnCacher
  , _rmCacheReader :: Maybe RemoteCacheReader
  }

data RConstInfo = MkRConstInfo {
  _rcstCacher :: RemoteCacher
  , _rcstUnCacher :: UnCacher
  , _rcstCacheReader :: Maybe RemoteCacheReader
  }

data LExpInfo = MkLExpInfo {
  _lexpCacher :: LocalCacher
  , _lexpUnCacher :: UnCacher
  }

type InfoMap = GenericInfoMap NodeTypeInfo



getVal :: (Monad m) =>  CachedValType -> V.Vault -> V.Key a -> EitherT RemoteClosureResult m a
getVal cvt vault key =
  case V.lookup key vault of
  Just v -> right v
  Nothing -> left $ RcRespCacheMiss cvt

getLocalVal :: (Monad m) =>  CachedValType -> V.Vault -> V.Key a -> EitherT RemoteClosureResult m a
getLocalVal cvt vault key  =
  case V.lookup key vault of
  Just v -> right v
  Nothing -> left $ RcRespCacheMiss cvt

makeUnCacher :: V.Key a -> V.Vault -> V.Vault
makeUnCacher k vault = V.delete k vault

mkRemoteClosure :: forall a b m . (MonadLoggerIO m) =>
  V.Key (Data a) -> V.Key (Data b) -> ExpClosure SExp a b -> StateT InfoMap m RemoteClosureImpl
mkRemoteClosure keya keyb (ExpClosure e f) = do
  analyseLocal e
  addLocalExpCacheM e
  let keyc = getLocalVaultKey e
  return $ wrapClosure keyc keya keyb f


wrapClosure :: forall a b c .
            V.Key c -> V.Key (Data a) -> V.Key (Data b) -> (c -> a -> IO b) -> RemoteClosureImpl
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
        brdd <- liftIO $ f' c av
        let vault' = V.insert keyb brdd vault
        return (RcRespOk, vault')
      f' _ NoData = return NoData
      f' c (Data a) = do
        x <-  f c a
        return $ Data x

visitLocalExp :: Int -> InfoMap -> InfoMap
visitLocalExp n m =
  case M.lookup n m of
    Just (GenericInfo _ _ ) -> m
    Nothing -> M.insert n (GenericInfo S.empty NtLExpNoCache) m



visitLocalExpM :: (MonadLoggerIO m) => Int -> StateT InfoMap m ()
visitLocalExpM n = do
  $(logInfo) $ T.pack  ("Visiting local exp node: " ++ show n)
  m <- get
  put $ visitLocalExp n m


addLocalExpCache :: (S.Serialize a) => Int -> V.Key a -> InfoMap -> InfoMap
addLocalExpCache n key m =
  case M.lookup n m of
  Just (GenericInfo c NtLExpNoCache) -> M.insert n (GenericInfo c (NtLExp (MkLExpInfo (makeCacher key) (makeUnCacher key)))) m
  Nothing -> M.insert n (GenericInfo S.empty (NtLExp (MkLExpInfo (makeCacher key) (makeUnCacher key)))) m
  Just (GenericInfo _ (NtLExp _)) -> m
  _ ->  error ("Node " ++ show n ++ " cannot add local exp cache")
  where
  makeCacher :: (S.Serialize a) => V.Key a -> BS.ByteString -> V.Vault -> V.Vault
  makeCacher k bs vault =
    case S.decode bs of
    Left e -> error $ ("Cannot deserialize value: " ++ e)
    Right a -> V.insert k a vault

addLocalExpCacheM :: forall a m. (MonadLoggerIO m, S.Serialize a) =>
  SExp 'Local a -> StateT InfoMap m ()
addLocalExpCacheM e = do
  let n = getLocalIndex e
  $(logInfo) $ T.pack  ("Adding cache to local exp node: " ++ show n)
  let key = getLocalVaultKey e
  m <- get
  put $ addLocalExpCache n key m

addRemoteExpCacheReader :: (S.Serialize a) => Int -> V.Key (Data a) -> InfoMap -> InfoMap
addRemoteExpCacheReader n key m =
  case M.lookup n m of
  Just (GenericInfo _ (NtRMap (MkRMapInfo _ _ (Just _)))) -> m
  Just (GenericInfo c (NtRMap (MkRMapInfo cs uncacher Nothing))) ->
    M.insert n (GenericInfo c (NtRMap (MkRMapInfo cs uncacher (Just cacheReader)))) m
  Just (GenericInfo _ (NtRConst (MkRConstInfo _ _ (Just _)))) -> m
  Just (GenericInfo c (NtRConst (MkRConstInfo cacher uncacher Nothing))) ->
    M.insert n (GenericInfo c (NtRConst (MkRConstInfo cacher uncacher (Just cacheReader)))) m
  _ ->  error ("Node " ++ show n ++ " cannot add remote exp cache reader")
  where
  cacheReader :: V.Vault -> Maybe (Data BS.ByteString)
  cacheReader vault =
    case V.lookup key vault of
      Nothing -> Nothing
      Just NoData -> Just NoData
      Just (Data b) -> Just $ Data $ S.encode b


addRemoteExpCacheReaderM ::
  forall a m. (MonadLoggerIO m, S.Serialize a)
  => SExp 'Remote a -> StateT InfoMap m ()
addRemoteExpCacheReaderM e = do
  let n = getRemoteIndex e
  $(logInfo) $ T.pack  ("Adding cache reader to remote exp node: " ++ show n)
  let key = getRemoteVaultKey e
  m <- get
  put $ addRemoteExpCacheReader n key m



getRemoteIndex :: SExp 'Remote a -> Int
getRemoteIndex (SRApply i _ _ _) = i
getRemoteIndex (SRConst i _ _) = i

getRemoteVaultKey :: SExp 'Remote a -> V.Key (Data a)
getRemoteVaultKey (SRApply _ k _ _) = k
getRemoteVaultKey (SRConst _ k _) = k

getLocalIndex :: SExp 'Local a -> Int
getLocalIndex (SLConst i _ _) = i
getLocalIndex (SCollect i _ _) = i
getLocalIndex (SLApply i _ _ _) = i

getLocalVaultKey :: SExp 'Local a -> V.Key a
getLocalVaultKey (SLConst _ k _) = k
getLocalVaultKey (SCollect _ k _) = k
getLocalVaultKey (SLApply _ k _ _) = k


analyseRemote :: (MonadLoggerIO m) => SExp 'Remote a -> StateT InfoMap m ()
analyseRemote (SRApply n keyb cs@(ExpClosure ce _) a) = do
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    referenceM n (getRemoteIndex a)
    analyseLocal ce
    referenceM n (getLocalIndex ce)
    $(logInfo) $ T.pack ("create closure for RApply node " ++ show n)
    let keya = getRemoteVaultKey a
    rcs <- mkRemoteClosure keya keyb cs
    visitRApplyM rcs
  where
  visitRApplyM cs' = do
    $(logInfo) $ T.pack  ("Visiting RMap node: " ++ show n)
    m <- get
    put $ visitRApply cs' m
  visitRApply cs' m =
    case M.lookup n m of
    Just (GenericInfo _ _) -> m
    Nothing -> M.insert n (GenericInfo S.empty (NtRMap (MkRMapInfo cs' (makeUnCacher keyb) Nothing))) m

analyseRemote (SRConst n key _) = do
  unlessM (wasVisitedM n) $ visitRConstExpM
  where
  visitRConstExpM = do
    $(logInfo) $ T.pack  ("Visiting RConst node: " ++ show n)
    m <- get
    put $ visitRConst m
  visitRConst m =
    case M.lookup n m of
    Just (GenericInfo _ _) -> error ("RConst Node " ++ show n ++ " has already been visited")
    Nothing -> M.insert n (GenericInfo S.empty (NtRConst (MkRConstInfo (makeCacher key) (makeUnCacher key) Nothing))) m
    where
    makeCacher :: (S.Serialize a) => V.Key (Data a) -> (Data BS.ByteString) -> V.Vault -> V.Vault
    makeCacher k (NoData) vault = V.insert k NoData vault
    makeCacher k (Data bs) vault =
      case S.decode bs of
      Left e -> error $ ("Cannot deserialize value: " ++ e)
      Right a -> V.insert k (Data a) vault



analyseLocal :: (MonadLoggerIO m) => SExp 'Local a -> StateT InfoMap m ()

analyseLocal(SLConst n _ _) = do
  unlessM (wasVisitedM n) $ visitLocalExpM n

analyseLocal (SCollect n _ e) = do
  unlessM (wasVisitedM n) $ do
    analyseRemote e
    addRemoteExpCacheReaderM e
    referenceM n (getRemoteIndex e)
    visitLocalExpM n

analyseLocal (SLApply n _ f e) = do
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal e
    visitLocalExpM n





combineClosure :: MonadLoggerIO m =>
                        Int
                        -> ExpClosure SExp a b
                        -> ExpClosure SExp b c
                        -> m (ExpClosure SExp a c, Int)
combineClosure counter (ExpClosure cf f) (ExpClosure cg g)  = do
  (cfg, counter') <- combineFreeVars counter cf cg
  let cs = ExpClosure cfg (\(cf', cg') a -> do
              r1 <- f cf' a
              g cg' r1)
  return (cs, counter')

combineFreeVars :: MonadLoggerIO m =>
                         Int
                         -> SExp 'Local a -> SExp 'Local a1 -> m (SExp 'Local (a, a1), Int)
combineFreeVars counter cf cg = do
    k1 <- liftIO $ V.newKey
    let f = SLConst counter k1 (,)
    k2 <- liftIO $ V.newKey
    let fcf = SLApply (counter+1) k2 f cf
    k3 <- liftIO $ V.newKey
    let cfg = SLApply (counter+2) k3 fcf cg
    return (cfg, counter+3)



fuseRemote :: (MonadLoggerIO m) => GenericInfoMap () -> Int -> SExp 'Remote a -> m (SExp 'Remote a, GenericInfoMap (), Int)
fuseRemote infos counter oe@(SRApply _ _ _ ie) | refCountInner > 1 =
    return (oe, infos, counter)
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos counter (SRApply ne key g (SRApply ni _ f e)) = do
    $(logInfo) $ T.pack ("Fusing SRApply " ++ show ne ++ " with SRApply " ++ show ni)
    (fg, counter') <- combineClosure counter f g
    return (SRApply ne key fg e, infos, counter')


fuseRemote infos counter oe = return (oe, infos, counter)





