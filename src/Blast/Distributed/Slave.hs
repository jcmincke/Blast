{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Distributed.Slave

where

import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State

import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V


import Blast.Internal.Types
import Blast.Distributed.Types
import Blast.Analyser
import Blast.Optimizer




data LocalSlave m a b = MkLocalSlave {
  localSlaveId :: Int
  , infos :: M.Map Int Info
  , vault :: V.Vault
  , expGen :: a -> StateT Int m (LocalExp (a, b))
  , config :: Config
  }



runCommand :: (S.Serialize a, MonadLoggerIO m) => LocalSlave m a b -> LocalSlaveRequest -> m (LocalSlaveResponse, LocalSlave m a b)
runCommand ls@(MkLocalSlave {..}) (LsReqReset  bs) = do
  case S.decode bs of
    Left e -> error e
    Right a -> do
      (e, count) <- runStateT (expGen a) 0
      infos1 <- execStateT (analyseLocal e) M.empty
      (infos2, _) <- if (shouldOptimize config)
                        then optimize count infos1 e
                        else return (infos1, e)
      let ls' = ls {infos = infos2, vault = V.empty}
      return  (LsRespVoid, ls')
runCommand ls LsReqStatus = return (LsRespBool (not $ M.null $ infos ls), ls)
runCommand ls (LsReqExecute i crv arv rdesc) = do
    case M.lookup i (infos ls) of
      Nothing -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ (Just cs) _) -> do
        (res, vault') <- liftIO $ cs (vault ls) crv arv rdesc
        let ls' = ls {vault =  vault'}
        return (LocalSlaveExecuteResult res, ls')
      Just (Info _ Nothing _) -> return (LocalSlaveExecuteResult (ExecResError ("closure not found: "++show i)), ls)
runCommand ls (LsReqCache i bs) =
    case M.lookup i (infos ls) of
      Nothing -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ _ (Just (MkCacheInfo cacherFun _ _))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespBool True, ls {vault = vault'})
runCommand ls (LsReqUncache i) = do
    case M.lookup i (infos ls) of
      Nothing -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ _ (Just (MkCacheInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
runCommand ls (LsReqIsCached i) = do
    case M.lookup i (infos ls) of
      Nothing -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
      Just (Info _ _ (Just (MkCacheInfo _ _ isCachedFun))) -> do
        let b = isCachedFun (vault ls)
        return (LsRespBool b, ls)



