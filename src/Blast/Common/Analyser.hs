{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Common.Analyser
where

--import Debug.Trace

import            Control.DeepSeq
import            Control.Lens (set, view, makeLenses)
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import            Data.Binary (Binary)
import qualified  Data.Map as M
import qualified  Data.Set as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V
import            GHC.Generics (Generic)




data CachedValType = CachedArg | CachedFreeVar
  deriving (Show, Generic)

data RemoteClosureResult =
  RemCsResCacheMiss CachedValType
  |ExecRes
  |ExecResError String
  deriving (Generic, Show)


instance NFData RemoteClosureResult
instance NFData CachedValType

instance Binary RemoteClosureResult
instance Binary CachedValType

type RemoteClosureImpl = V.Vault -> IO (RemoteClosureResult, V.Vault)


data GenericInfo i = GenericInfo {
  _refs :: S.Set Int -- set of parents, that is, nodes that reference this node
  , _info :: i
  }

$(makeLenses ''GenericInfo)

type GenericInfoMap i = M.Map Int (GenericInfo i)

refCount :: Int -> GenericInfoMap i -> Int
refCount n m =
  case M.lookup n m of
    Just inf -> S.size $ view refs inf
    Nothing -> error ("Ref count not found for node: " ++ show n)



referenceM :: forall i m. MonadLoggerIO m =>
                Int -> Int -> StateT (GenericInfoMap i) m ()
referenceM parent child = do
  $(logInfo) $ T.pack ("Parent node "++show parent ++ " references child node " ++ show child)
  m <- get
  put (reference m)
  where
  reference m =
    case M.lookup child m of
    Just inf@(GenericInfo old _) -> M.insert child (set refs (S.insert parent old) inf) m
    Nothing -> error $  ("Node " ++ show child ++ " is referenced before being visited")



wasVisitedM ::  forall i m. Monad m =>
                Int -> StateT (GenericInfoMap i) m Bool
wasVisitedM n = do
  m <- get
  return $ M.member n m



