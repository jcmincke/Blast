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
runCommand (LsReqReset bs) ls@(MkSlaveContext {..}) = do
  case S.decode bs of
    Left e -> error e
    Right a -> do
      let program = expGen a
      (refMap, count) <- generateReferenceMap 0 M.empty program
      (e::SExp 'Local (a,b)) <- build (shouldOptimize config) refMap (0::Int) count program
      infos' <- execStateT (analyseLocal e) M.empty
      let ls' = ls {infos = infos', vault = V.empty}
      return  (LsRespVoid, ls')
runCommand (LsReqExecute i) ls = do
    case M.lookup i (infos ls) of
      Just  (GenericInfo _ (NtRMap (MkRMapInfo cs _ _))) -> do
        (res, vault') <- liftIO $ cs (vault ls)
        let ls' = ls {vault =  vault'}
        case res of
          RcRespError err -> return (LsRespError err, ls')
          _ -> return (LsRespExecute res, ls')
      _ -> return (LsRespError ("Info not found: "++show i), ls)
runCommand (LsReqCache i bs) ls =
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRConst (MkRConstInfo cacherFun _ _))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespVoid, ls {vault = vault'})

      Just (GenericInfo _ (NtLExp (MkLExpInfo cacherFun _ ))) -> do
        let vault' = cacherFun bs (vault ls)
        return (LsRespVoid, ls {vault = vault'})

      Just (GenericInfo _ (NtRMap _)) -> return (LsRespError ("NtRMap GenericInfo not found: "++show i), ls)
      Just (GenericInfo _ (NtLExpNoCache)) -> return (LsRespError ("NtLExpNoCache GenericInfo not found: "++show i), ls)
      _ -> return (LsRespError ("Nothing : GenericInfo not found: "++show i), ls)
runCommand (LsReqUncache i) ls = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespVoid, ls {vault = vault'})
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ unCacherFun _))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespVoid, ls {vault = vault'})
      Just (GenericInfo _ (NtLExp (MkLExpInfo _ unCacherFun))) -> do
        let vault' = unCacherFun (vault ls)
        return (LsRespVoid, ls {vault = vault'})
      _ -> return (LsRespError ("GenericInfo not found: "++show i), ls)
runCommand (LsReqFetch i) ls = do
    case M.lookup i (infos ls) of
      Just (GenericInfo _ (NtRMap (MkRMapInfo _ _ (Just cacheReaderFun)))) -> do
        case cacheReaderFun (vault ls) of
          Just a -> return (LsRespFetch a, ls)
          Nothing -> return (LsRespError "Cannot fetch results", ls)
      Just (GenericInfo _ (NtRConst (MkRConstInfo _ _ (Just cacheReaderFun)))) -> do
        case cacheReaderFun (vault ls) of
          Just a -> return (LsRespFetch a, ls)
          Nothing -> return (LsRespError "Cannot fetch results", ls)
      _ -> return $ (LsRespError "Cannot fetch results", ls)
runCommand (LsReqBatch nRes requests) ls = do
  ls' <- foldM (\acc req -> do  (_, acc') <- runCommand req acc
                                return acc') ls requests
  -- fetch results
  (res, ls'') <- runCommand (LsReqFetch nRes) ls'
  case res of
    LsRespFetch r -> return $ (LsRespBatch r, ls'')
    LsRespError err -> return $ (LsRespError err, ls'')
    _ -> return $ (LsRespError "Batch: bad response", ls'')

