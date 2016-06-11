{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Common.Analyser
where

import Debug.Trace
import            Control.Bool (unlessM)
import            Control.Lens (set, view, makeLenses)
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




data Info i = Info {
  _nbRef :: Int
  , _info :: i
  }

$(makeLenses ''Info)

type GenericInfoMap i = M.Map Int (Info i)

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
    Just info@(Info old _) -> M.insert n (set nbRef (old+1) info) m
    Nothing -> error $  ("Node " ++ show n ++ " is referenced before being visited")



wasVisitedM ::  forall i m. Monad m =>
                Int -> StateT (GenericInfoMap i) m Bool
wasVisitedM n = do
  m <- get
  return $ M.member n m



