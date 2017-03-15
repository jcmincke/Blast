{-
Copyright   : (c) Jean-Christophe Mincke, 2016-2017

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}



{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Control.Distributed.Blast.Master.Analyser
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

import            Control.Distributed.Blast.Types
import            Control.Distributed.Blast.Common.Analyser

type InfoMap = GenericInfoMap ()

type LocalKey a = V.Key (a, Maybe (Partition BS.ByteString))

data MExp (k::Kind) a where
  MRApply :: Int -> ExpClosure MExp a b -> MExp 'Remote a -> MExp 'Remote b
  MRConst :: (S.Serialize b) => Int -> V.Key (Partition BS.ByteString) -> ChunkFun a b -> IO a -> MExp 'Remote b
  MLConst :: Int -> LocalKey a -> IO a -> MExp 'Local a
  MCollect :: (S.Serialize b) => Int -> LocalKey a -> UnChunkFun b a -> MExp 'Remote b -> MExp 'Local a
  MLApply :: Int -> LocalKey b -> MExp 'Local (a -> b) -> MExp 'Local a -> MExp 'Local b



instance (MonadLoggerIO m) => Builder m MExp where
  makeRApply i f a = do
    return $ MRApply i f a
  makeRConst i chunkFun a = do
    k <- liftIO V.newKey
    return $ MRConst i k chunkFun a
  makeLConst i a = do
    k <- liftIO V.newKey
    return $ MLConst i k a
  makeCollect i unChunkFun a = do
    k <- liftIO V.newKey
    return $ MCollect i k unChunkFun a
  makeLApply i f a = do
    k <- liftIO V.newKey
    return $ MLApply i k f a




instance Indexable MExp where
  getIndex (MRApply n _ _) = n
  getIndex (MRConst n _ _ _) = n
  getIndex (MLConst n _ _) = n
  getIndex (MCollect n _ _ _) = n
  getIndex (MLApply n _ _ _) = n




getRemoteIndex :: MExp 'Remote a -> Int
getRemoteIndex (MRApply i _ _) = i
getRemoteIndex (MRConst i _ _ _) = i

getLocalIndex :: MExp 'Local a -> Int
getLocalIndex (MLConst i _ _) = i
getLocalIndex (MCollect i _ _ _) = i
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



analyseRemote e@(MRConst n _ _ _) = unlessM (wasVisitedM n) $ visitRemoteM e


analyseLocal :: (MonadLoggerIO m) => MExp 'Local a -> StateT InfoMap m ()

analyseLocal e@(MLConst n _ _) = unlessM (wasVisitedM n) $ visitLocalM e

analyseLocal e@(MCollect n _ _ a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    referenceM n (getRemoteIndex a)
    visitLocalM e

analyseLocal e@(MLApply n _ f a) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal a
    visitLocalM e



