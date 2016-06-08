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

import Debug.Trace
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
runCommand ls (LsReqExecute i ) = do
    case M.lookup i (infos ls) of
      Just  (Info _ (NtRMap (MkRMapInfo cs _ _))) -> do
        (res, vault') <- liftIO $ cs (vault ls)
        let ls' = ls {vault =  vault'}
        return (LocalSlaveExecuteResult res, ls')
      _ -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
runCommand ls (LsReqCache i bs) =
    case M.lookup i (infos ls) of
      Just (Info _ (NtRConst (MkRConstInfo cacherFun _ _))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      Just (Info _ (NtLExp (MkLExpInfo cacherFun _))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      _ -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
runCommand ls (LsReqUncache i) = do
    case M.lookup i (infos ls) of
      Just (Info _ (NtRMap (MkRMapInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      Just (Info _ (NtRConst (MkRConstInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      Just (Info _ (NtLExp (MkLExpInfo _ unCacherFun))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      _ -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
runCommand ls (LsReqFetch i) = do
    case M.lookup i (infos ls) of
      Just (Info _ (NtRMap (MkRMapInfo _ _ (Just cacheReaderFun)))) -> do
        return (LsFetch $ cacheReaderFun (vault ls), ls)
      Just (Info _ (NtRConst (MkRConstInfo _ _ (Just cacheReaderFun)))) -> do
        return (LsFetch $ cacheReaderFun (vault ls), ls)
      _ -> return $ trace ("no cache reader for node: " ++ show i) (LsFetch Nothing, ls)


