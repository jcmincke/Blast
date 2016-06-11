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
import            GHC.Generics (Generic)

import            Blast.Types
import            Blast.Common.Analyser

type InfoMap = GenericInfoMap ()

type LocalKey a = V.Key (a, Maybe (Partition BS.ByteString))

data MExp (k::Kind) a where
  MRApply :: Int -> ExpClosure MExp a b -> MExp 'Remote a -> MExp 'Remote b
  MRConst :: Int -> V.Key (Partition BS.ByteString) -> a -> MExp 'Remote a
  MLConst :: Int -> LocalKey a -> a -> MExp 'Local a
  MCollect :: Int -> LocalKey a -> MExp 'Remote a -> MExp 'Local a
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

analyseRemote :: (MonadLoggerIO m) => MExp 'Remote a -> StateT InfoMap m ()
analyseRemote (MRApply n cs@(ExpClosure ce _) a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    increaseRefM (getRemoteIndex a)
    analyseLocal ce
    increaseRefM (getLocalIndex ce)
    $(logInfo) $ T.pack ("create closure for RApply node " ++ show n)



analyseRemote (MRConst _ _ _) = return ()


analyseLocal :: (MonadLoggerIO m) => MExp 'Local a -> StateT InfoMap m ()

analyseLocal (MLConst n _ _) = return ()

analyseLocal (MCollect n _ a) =
  unlessM (wasVisitedM n) $ do
    analyseRemote a
    increaseRefM (getRemoteIndex a)

analyseLocal (MLApply n _ f a) =
  unlessM (wasVisitedM n) $ do
    analyseLocal f
    analyseLocal a

