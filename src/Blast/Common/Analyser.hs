{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Common.Analyser
(
  CachedValType (..)
  , RemoteClosureResult (..)
  , RemoteClosureImpl
  , Data (..)
  , referenceM
  , wasVisitedM
)
where

--import Debug.Trace

import            Control.DeepSeq
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import            Data.Binary (Binary)
import qualified  Data.Map as M
import qualified  Data.Set as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V
import            GHC.Generics (Generic)

import            Blast.Types


data CachedValType = CachedArg | CachedFreeVar
  deriving (Show, Generic)

data RemoteClosureResult =
  RcRespCacheMiss CachedValType
  |RcRespOk
  |RcRespError String
  deriving (Generic, Show)


instance NFData RemoteClosureResult
instance NFData CachedValType

instance Binary RemoteClosureResult
instance Binary CachedValType

type RemoteClosureImpl = V.Vault -> IO (RemoteClosureResult, V.Vault)


data Data a =
  Data a
  |NoData
  deriving (Show, Generic)

instance (Binary a) => Binary (Data a)
instance (NFData a) => NFData (Data a)

referenceM :: forall i m. MonadLoggerIO m =>
                Int -> Int -> StateT (GenericInfoMap i) m ()
referenceM parent child = do
  $(logInfo) $ T.pack ("Parent node "++show parent ++ " references child node " ++ show child)
  m <- get
  put (doReference m)
  where
  doReference m =
    case M.lookup child m of
    Just inf@(GenericInfo old _) -> M.insert child (inf {giRefs = S.insert parent old}) m
    Nothing -> error $  ("Node " ++ show child ++ " is referenced before being visited")



wasVisitedM ::  forall i m. Monad m =>
                Int -> StateT (GenericInfoMap i) m Bool
wasVisitedM n = do
  m <- get
  return $ M.member n m



