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
where

--import Debug.Trace
import            Control.Bool (unlessM)
import            Control.Monad.Logger
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V

import            Blast.Types
import            Blast.Common.Analyser

type InfoMap = GenericInfoMap ()

type LocalKey a = V.Key (a, Maybe (Partition BS.ByteString))

data MExp (k::Kind) a where
  MRApply :: Int -> ExpClosure MExp a b -> MExp 'Remote a -> MExp 'Remote b
  MRConst :: (Chunkable a, S.Serialize a) => Int -> V.Key (Partition BS.ByteString) -> a -> MExp 'Remote a
  MLConst :: Int -> LocalKey a -> a -> MExp 'Local a
  MCollect :: (UnChunkable a, S.Serialize a) => Int -> LocalKey a -> MExp 'Remote a -> MExp 'Local a
  MLApply :: Int -> LocalKey b -> MExp 'Local (a -> b) -> MExp 'Local a -> MExp 'Local b



nextIndex :: (MonadIO m) => StateT Int m Int
nextIndex = do
  index <- get
  put (index+1)
  return index


instance (MonadIO m) => Builder (StateT Int m) MExp where
  makeRApply f a = do
    i <- nextIndex
    return $ MRApply i f a
  makeRConst a = do
    i <- nextIndex
    k <- liftIO V.newKey
    return $ MRConst i k a
  makeLConst a = do
    i <- nextIndex
    k <- liftIO V.newKey
    return $ MLConst i k a
  makeCollect a = do
    i <- nextIndex
    k <- liftIO V.newKey
    return $ MCollect i k a
  makeLApply f a = do
    i <- nextIndex
    k <- liftIO V.newKey
    return $ MLApply i k f a




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
  Nothing -> M.insert n (GenericInfo 0 ()) m


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
    increaseRefM (getRemoteIndex a)
    analyseLocal ce
    increaseRefM (getLocalIndex ce)
    visitRemoteM e



analyseRemote e@(MRConst n _ _) = unlessM (wasVisitedM n) $ visitRemoteM e


analyseLocal :: (MonadLoggerIO m) => MExp 'Local a -> StateT InfoMap m ()

analyseLocal e@(MLConst n _ _) = unlessM (wasVisitedM n) $ visitLocalM e

analyseLocal e@(MCollect n _ a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    increaseRefM (getRemoteIndex a)
    visitLocalM e

analyseLocal e@(MLApply n _ f a) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal a
    visitLocalM e

