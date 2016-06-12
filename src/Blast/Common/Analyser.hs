{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Common.Analyser
where

import Debug.Trace

import            Control.Bool (unlessM)
import            Control.DeepSeq
import            Control.Lens (set, view, makeLenses)
import            Control.Monad.Logger
import            Control.Monad.IO.Class
import            Control.Monad.Trans.Either
import            Control.Monad.Trans.State
import            Data.Binary (Binary)
import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Text as T
import qualified  Data.Vault.Strict as V
import            GHC.Generics (Generic)

import            Blast.Types



data CachedValType = CachedArg | CachedFreeVar
  deriving (Show, Generic)

data RemoteClosureResult =
  RemCsResCacheMiss CachedValType
  |ExecRes
  |ExecResError String    --
  deriving (Generic, Show)


instance NFData RemoteClosureResult
instance NFData CachedValType

instance Binary RemoteClosureResult
instance Binary CachedValType

type RemoteClosureImpl = V.Vault -> IO (RemoteClosureResult, V.Vault)


data GenericInfo i = GenericInfo {
  _nbRef :: Int
  , _info :: i
  }

$(makeLenses ''GenericInfo)

type GenericInfoMap i = M.Map Int (GenericInfo i)

refCount :: Int -> GenericInfoMap i -> Int
refCount n m =
  case M.lookup n m of
    Just info -> view nbRef info
    Nothing -> error ("Ref count not found for node: " ++ show n)


increaseRefM :: forall i m. MonadLoggerIO m =>
                Int -> StateT (GenericInfoMap i) m ()
increaseRefM n = do
  $(logInfo) $ T.pack ("Referencing node: " ++ show n)
  m <- get
  put (increaseRef n m)
  where
  increaseRef :: Int -> GenericInfoMap i -> GenericInfoMap i
  increaseRef n m =
    case M.lookup n m of
    Just info@(GenericInfo old _) -> M.insert n (set nbRef (old+1) info) m
    Nothing -> error $  ("Node " ++ show n ++ " is referenced before being visited")



wasVisitedM ::  forall i m. Monad m =>
                Int -> StateT (GenericInfoMap i) m Bool
wasVisitedM n = do
  m <- get
  return $ M.member n m



