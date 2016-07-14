{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Master.Analyser
(
  MExp (..)
  , InfoMap
  , LocalKey
  , analyseLocal
  , getLocalIndex
  , getRemoteIndex
)
where

--import Debug.Trace
import            Control.Bool (unlessM)
import            Control.Monad.Logger
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Set as S
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V

import            Blast.Types
import            Blast.Common.Analyser

type InfoMap = GenericInfoMap ()

type LocalKey a = V.Key (a, Maybe (Partition BS.ByteString))

data MExp (k::Kind) a where
  MRApply :: Int -> ExpClosure MExp a b -> MExp 'Remote a -> MExp 'Remote b
  MRConst :: (Chunkable a b, S.Serialize b) => Int -> V.Key (Partition BS.ByteString) -> a -> MExp 'Remote b
  MLConst :: Int -> LocalKey a -> a -> MExp 'Local a
  MCollect :: (UnChunkable b a, S.Serialize b) => Int -> LocalKey a -> MExp 'Remote b -> MExp 'Local a
  MLApply :: Int -> LocalKey b -> MExp 'Local (a -> b) -> MExp 'Local a -> MExp 'Local b



instance (MonadLoggerIO m) => Builder m MExp where
  makeRApply i f a = do
    return $ MRApply i f a
  makeRConst i a = do
    k <- liftIO V.newKey
    return $ MRConst i k a
  makeLConst i a = do
    k <- liftIO V.newKey
    return $ MLConst i k a
  makeCollect i a = do
    k <- liftIO V.newKey
    return $ MCollect i k a
  makeLApply i f a = do
    k <- liftIO V.newKey
    return $ MLApply i k f a
  fuse refMap n e = fuseRemote refMap n e




instance Indexable MExp where
  getIndex (MRApply n _ _) = n
  getIndex (MRConst n _ _) = n
  getIndex (MLConst n _ _) = n
  getIndex (MCollect n _ _) = n
  getIndex (MLApply n _ _ _) = n




getRemoteIndex :: MExp 'Remote a -> Int
getRemoteIndex (MRApply i _ _) = i
getRemoteIndex (MRConst i _ _) = i

getLocalIndex :: MExp 'Local a -> Int
getLocalIndex (MLConst i _ _) = i
getLocalIndex (MCollect i _ _) = i
getLocalIndex (MLApply i _ _ _) = i



visit :: Int -> InfoMap -> InfoMap
visit n m =
  case M.lookup n m of
  Just (GenericInfo _ _) -> error ("Node " ++ show n ++ " has already been visited")
  Nothing -> M.insert n (GenericInfo S.empty ()) m


visitRemoteM :: forall a m. (MonadLoggerIO m) =>
          MExp 'Remote a -> StateT InfoMap m ()
visitRemoteM e = do
  let n = getRemoteIndex e
  $(logInfo) $ T.pack  ("Visiting node: " ++ show n)
  m <- get
  put $ visit n m

visitLocalM :: forall a m. (MonadLoggerIO m) =>
          MExp 'Local a -> StateT InfoMap m ()
visitLocalM e = do
  let n = getLocalIndex e
  $(logInfo) $ T.pack  ("Visiting node: " ++ show n)
  m <- get
  put $ visit n m


analyseRemote :: (MonadLoggerIO m) => MExp 'Remote a -> StateT InfoMap m ()
analyseRemote e@(MRApply n (ExpClosure ce _) a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    referenceM n (getRemoteIndex a)
    analyseLocal ce
    referenceM n (getLocalIndex ce)
    visitRemoteM e



analyseRemote e@(MRConst n _ _) = unlessM (wasVisitedM n) $ visitRemoteM e


analyseLocal :: (MonadLoggerIO m) => MExp 'Local a -> StateT InfoMap m ()

analyseLocal e@(MLConst n _ _) = unlessM (wasVisitedM n) $ visitLocalM e

analyseLocal e@(MCollect n _ a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    referenceM n (getRemoteIndex a)
    visitLocalM e

analyseLocal e@(MLApply n _ f a) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal a
    visitLocalM e



combineClosure :: MonadLoggerIO m =>
                        Int
                        -> ExpClosure MExp a b
                        -> ExpClosure MExp b c
                        -> m (ExpClosure MExp a c, Int)
combineClosure counter (ExpClosure cf f) (ExpClosure cg g)  = do
  (cfg, counter') <- combineFreeVars counter cf cg
  let cs = ExpClosure cfg (\(cf', cg') a -> do
              r1 <- f cf' a
              g cg' r1)
  return (cs, counter')

combineFreeVars :: MonadLoggerIO m =>
                         Int
                         -> MExp 'Local a -> MExp 'Local a1 -> m (MExp 'Local (a, a1), Int)
combineFreeVars counter cf cg = do
    k1 <- liftIO $ V.newKey
    let f = MLConst counter k1 (,)
    k2 <- liftIO $ V.newKey
    let fcf = MLApply (counter+1) k2 f cf
    k3 <- liftIO $ V.newKey
    let cfg = MLApply (counter+2) k3 fcf cg
    return (cfg, counter+3)



fuseRemote :: (MonadLoggerIO m) => GenericInfoMap () -> Int -> MExp 'Remote a -> m (MExp 'Remote a, GenericInfoMap (), Int)
fuseRemote infos counter oe@(MRApply _ _ ie) | refCountInner > 1 =
    return (oe, infos, counter)
    where
    refCountInner = refCount (getRemoteIndex ie) infos

fuseRemote infos counter (MRApply ne g (MRApply ni f e)) = do
    $(logInfo) $ T.pack ("Fusing MRApply " ++ show ne ++ " with MRApply " ++ show ni)
    (fg, counter') <- combineClosure counter f g
    return (MRApply ne fg e, infos, counter')


fuseRemote infos counter oe = return (oe, infos, counter)
