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
  , makeSlaveContext
)
where

--import Debug.Trace
import            Control.Monad
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



-- | Describes the current context of a slave.
data SlaveContext m a b = MkSlaveContext {
  localSlaveId :: Int
  , infos :: InfoMap
  , vault :: V.Vault
  , expGen :: a -> ProgramT (Syntax m) m (SExp 'Local (a, b))
  , config :: Config
  }

-- | Creates a "SlaveContext" for a given slave.
makeSlaveContext :: (MonadLoggerIO m)
  => Config               -- ^ Configuration
  -> Int                  -- ^Index of the slave.
  -> JobDesc a b          -- ^ Job description
  -> SlaveContext m a b   -- ^ Slave Context
makeSlaveContext config slaveId (MkJobDesc {..}) =
  MkSlaveContext slaveId M.empty V.empty computationGen config

-- | Runs the given command against the specified state of a slave.
runCommand :: forall a b m. (S.Serialize a, MonadLoggerIO m)
  => SlaveRequest                          -- ^ Command.
  -> SlaveContext m a b                    -- ^ Slave context
  -> m (SlaveResponse, SlaveContext m a b) -- ^ Returns the response from the slave and the new slave context.
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

