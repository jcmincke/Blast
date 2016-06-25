{-# LANGUAGE DataKinds #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Distributed.Slave
(
  LocalSlave (..)
  , runCommand
)
where

--import Debug.Trace
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Operational
import            Control.Monad.Trans.State

import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V


import Blast.Types
import Blast.Distributed.Types
import Blast.Common.Analyser
import Blast.Slave.Analyser




data LocalSlave m a b = MkLocalSlave {
  localSlaveId :: Int
  , infos :: InfoMap
  , vault :: V.Vault
  , expGen :: a -> ProgramT (Syntax m) m (SExp 'Local (a, b))
  , config :: Config
  }


runCommand :: forall a b m. (S.Serialize a, MonadLoggerIO m) => LocalSlave m a b -> LocalSlaveRequest -> m (LocalSlaveResponse, LocalSlave m a b)
runCommand ls@(MkLocalSlave {..}) (LsReqReset  bs) = do
  case S.decode bs of
    Left e -> error e
    Right a -> do
      let program = expGen a
      (refMap, count) <- generateReferenceMap 0 M.empty program
      (e::SExp 'Local (a,b)) <- build (shouldOptimize config) refMap (0::Int) (1000::Int) program

--      ((e::SExp 'Local (a,b)), _) <- runStateT (expGen a) 0
      infos' <- execStateT (analyseLocal e) M.empty
      let ls' = ls {infos = infos', vault = V.empty}
      return  (LsRespVoid, ls')
runCommand ls LsReqStatus = return (LsRespBool (not $ M.null $ infos ls), ls)
runCommand ls (LsReqExecute i ) = do
    case M.lookup i (infos ls) of
      Just  (GenericInfo _ (NtRMap (MkRMapInfo cs _ _))) -> do
        (res, vault') <- liftIO $ cs (vault ls)
        let ls' = ls {vault =  vault'}
        return (LocalSlaveExecuteResult res, ls')
      _ -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
runCommand ls (LsReqCache i bs) =
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRConst (MkRConstInfo cacherFun _ _))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespBool True, ls {vault = vault'})

      Just (GenericInfo _ (NtLExp (MkLExpInfo cacherFun _ ))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespBool True, ls {vault = vault'})

      Just (GenericInfo _ (NtRMap _)) -> return (LocalSlaveExecuteResult (ExecResError ("NtRMap GenericInfo not found: "++show i)), ls)
      Just (GenericInfo _ (NtLExpNoCache)) -> return (LocalSlaveExecuteResult (ExecResError ("NtLExpNoCache GenericInfo not found: "++show i)), ls)
      _ -> return (LocalSlaveExecuteResult (ExecResError ("Nothing : GenericInfo not found: "++show i)), ls)
runCommand ls (LsReqUncache i) = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      Just (GenericInfo _ (NtLExp (MkLExpInfo _ unCacherFun))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespBool True, ls {vault = vault'})
      _ -> return (LocalSlaveExecuteResult (ExecResError ("GenericInfo not found: "++show i)), ls)
runCommand ls (LsReqFetch i) = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ _ (Just cacheReaderFun)))) -> do
        return (LsFetch $ cacheReaderFun (vault ls), ls)
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ _ (Just cacheReaderFun)))) -> do
        return (LsFetch $ cacheReaderFun (vault ls), ls)
      _ -> return $ (LsFetch Nothing, ls)


