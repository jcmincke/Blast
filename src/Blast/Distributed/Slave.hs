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
  SlaveContext (..)
  , runCommand
)
where

--import Debug.Trace
import            Control.Monad
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Operational
import            Control.Monad.Trans.State

import qualified  Data.ByteString as BS
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V


import Blast.Types
import Blast.Distributed.Types
import Blast.Common.Analyser
import Blast.Slave.Analyser




data SlaveContext m a b = MkSlaveContext {
  localSlaveId :: Int
  , infos :: InfoMap
  , vault :: V.Vault
  , expGen :: a -> ProgramT (Syntax m) m (SExp 'Local (a, b))
  , config :: Config
  }

{-
slaveReset :: (S.Serialize a, MonadLoggerIO m) => BS.ByteString -> LocalSlave m a b -> m (LocalSlave m a b)
slaveReset bs ls@(MkLocalSlave {..}) = do
  case S.decode bs of
    Left e -> error e
    Right a -> do
      let program = expGen a
      (refMap, count) <- generateReferenceMap 0 M.empty program
      (e::SExp 'Local (a,b)) <- build (shouldOptimize config) refMap (0::Int) count program
      infos' <- execStateT (analyseLocal e) M.empty
      let ls' = ls {infos = infos', vault = V.empty}
      return ls'

slaveStatus :: Monad m => LocalSlave m a b -> m (Bool, LocalSlave m a b)
slaveStatus ls  = return (not $ M.null $ infos ls, ls)

slaveExecute :: MonadIO m => Int -> LocalSlave m a b -> m (RemoteClosureResult, LocalSlave m a b)
slaveExecute i ls = do
    case M.lookup i (infos ls) of
      Just  (GenericInfo _ (NtRMap (MkRMapInfo cs _ _))) -> do
        (res, vault') <- liftIO $ cs (vault ls)
        let ls' = ls {vault =  vault'}
        return (res, ls')
      _ -> return (ExecResError  ("info not found: "++show i), ls)


slaveCache :: Monad m => Int -> BS.ByteString -> LocalSlave m a b -> m (Maybe String, LocalSlave m a b)
slaveCache i bs ls =
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRConst (MkRConstInfo cacherFun _ _))) -> do
        let vault' = cacherFun bs (vault ls)
        return (Nothing, ls {vault = vault'})

      Just (GenericInfo _ (NtLExp (MkLExpInfo cacherFun _ ))) -> do
        let vault' = cacherFun bs (vault ls)
        return (Nothing, ls {vault = vault'})

      Just (GenericInfo _ (NtRMap _)) -> return (Just ("NtRMap GenericInfo not found: "++show i), ls)
      Just (GenericInfo _ (NtLExpNoCache)) -> return (Just ("NtLExpNoCache GenericInfo not found: "++show i), ls)
      _ -> return (Just ("Nothing : GenericInfo not found: "++show i), ls)

slaveUnCache :: Monad m => Int -> LocalSlave m a b -> m (Maybe String, LocalSlave m a b)
slaveUnCache i ls = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (Nothing, ls {vault = vault'})
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (Nothing, ls {vault = vault'})
      Just (GenericInfo _ (NtLExp (MkLExpInfo _ unCacherFun))) -> do
        let vault' = unCacherFun (vault ls)
        return (Nothing, ls {vault = vault'})
      _ -> return (Just ("GenericInfo not found: "++show i), ls)

slaveFetch :: Monad m
  => Int
  -> LocalSlave m a b
  -> m (Maybe BS.ByteString, LocalSlave m a b)
slaveFetch i ls = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ _ (Just cacheReaderFun)))) -> do
        return (cacheReaderFun (vault ls), ls)
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ _ (Just cacheReaderFun)))) -> do
        return (cacheReaderFun (vault ls), ls)
      _ -> return $ (Nothing, ls)
slaveBatch ::(Monad m)
    => Int
    -> [LocalSlave m a b -> m (t, LocalSlave m a b)]
    -> LocalSlave m a b
    -> m (Either String BS.ByteString, LocalSlave m a b)
slaveBatch nRes requests ls = do
  ls' <- foldM (\acc request -> do  (_, acc') <- request acc
                                    return acc') ls requests
  -- fetch results
  (res, ls'') <- slaveFetch nRes ls'
  case res of
    Just r -> return $ (Right r, ls'')
    Nothing -> return $ (Left "Batch: could not read results", ls'')
    _ -> return $ (Left "Batch: bad response", ls'')
-}

runCommand :: forall a b m. (S.Serialize a, MonadLoggerIO m) => LocalSlaveRequest -> SlaveContext m a b -> m (LocalSlaveResponse, SlaveContext m a b)
runCommand (LsReqReset  bs) ls@(MkSlaveContext {..}) = do
  case S.decode bs of
    Left e -> error e
    Right a -> do
      let program = expGen a
      (refMap, count) <- generateReferenceMap 0 M.empty program
      (e::SExp 'Local (a,b)) <- build (shouldOptimize config) refMap (0::Int) count program
      infos' <- execStateT (analyseLocal e) M.empty
      let ls' = ls {infos = infos', vault = V.empty}
      return  (LsRespVoid, ls')
runCommand LsReqStatus ls = return (LsRespBool (not $ M.null $ infos ls), ls)
runCommand (LsReqExecute i) ls = do
    case M.lookup i (infos ls) of
      Just  (GenericInfo _ (NtRMap (MkRMapInfo cs _ _))) -> do
        (res, vault') <- liftIO $ cs (vault ls)
        let ls' = ls {vault =  vault'}
        return (LocalSlaveExecuteResult res, ls')
      _ -> return (LocalSlaveExecuteResult (ExecResError ("info not found: "++show i)), ls)
runCommand (LsReqCache i bs) ls =
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
runCommand (LsReqUncache i) ls = do
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
runCommand (LsReqFetch i) ls = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ _ (Just cacheReaderFun)))) -> do
        return (LsFetch $ cacheReaderFun (vault ls), ls)
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ _ (Just cacheReaderFun)))) -> do
        return (LsFetch $ cacheReaderFun (vault ls), ls)
      _ -> return $ (LsFetch Nothing, ls)
runCommand (LsReqBatch nRes requests) ls = do
  ls' <- foldM (\acc req -> do  (_, acc') <- runCommand req acc
                                return acc') ls requests
  -- fetch results
  (res, ls'') <- runCommand (LsReqFetch nRes) ls'
  case res of
    (LsFetch (Just r)) -> return $ (LsRespBatch (Right r), ls'')
    (LsFetch Nothing) -> return $ (LsRespBatch (Left "Batch: could not read results"), ls'')
    _ -> return $ (LsRespBatch (Left "Batch: bad response"), ls'')

