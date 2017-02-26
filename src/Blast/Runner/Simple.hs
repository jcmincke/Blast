{-
Copyright   : (c) Jean-Christophe Mincke, 2016-2017

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
-}


{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Runner.Simple
(
  runRec
)
where

import Debug.Trace
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import qualified  Data.Map as M
import qualified  Data.Vector as Vc

import            Blast.Types

data Exp (k::Kind) a where
  RApply :: Int -> ExpClosure Exp a b -> Exp 'Remote a -> Exp 'Remote b
  RConst :: Int -> ChunkFun a b ->  a -> Exp 'Remote b
  LConst :: Int -> a -> Exp 'Local a
  Collect :: Int -> UnChunkFun b a -> Exp 'Remote b -> Exp 'Local a
  LApply :: Int -> Exp 'Local (a -> b) -> Exp 'Local a -> Exp 'Local b


instance (Monad m) => Builder m Exp where
  makeRApply n f a = do
    return $ RApply n f a
  makeRConst n f a = do
    return $ RConst n f a
  makeLConst n a = do
    return $ LConst n a
  makeCollect n f a = do
    return $ Collect n f a
  makeLApply n f a = do
    return $ LApply n f a



instance Indexable Exp where
  getIndex (RApply n _ _) = n
  getIndex (RConst n _ _ ) = n
  getIndex (LConst n _) = n
  getIndex (Collect n _ _) = n
  getIndex (LApply n _ _) = n





-- | Runs a computation using a simple interpreter. Execute all computations on just one thread.
runRec :: forall a b m. (MonadLoggerIO m) =>
  JobDesc a b
  -> m (a, b)
runRec (jobDesc@MkJobDesc {..}) = do
  let program = computationGen seed
  (refMap, count) <- generateReferenceMap 0 M.empty program
  !(e::Exp 'Local (a,b)) <- build refMap count program
  (a,b) <- liftIO $ runLocal e
  a' <- liftIO $ reportingAction a b
  case shouldStop a a' b of
    True -> do
      return (a', b)
    False -> do
      runRec (jobDesc {seed = a'})



runFun :: ExpClosure Exp a b -> IO (a -> IO b)
runFun (ExpClosure e f) = do
  r <- runLocal e
  return $ f r



runRemote :: Exp 'Remote a -> IO a
runRemote (RApply _ cs e) = do
  f' <- runFun cs
  e' <- runRemote e
  trace "RApply" $ f' e'

runRemote (RConst _ chunkFun e) =
  return (chunkFun 1 e Vc.! 0)
  where


runLocal ::  Exp 'Local a -> IO a
runLocal (Collect _ unChunkFun e) = do
  b <- runRemote e
  return $ unChunkFun [b]
runLocal (LConst _ a) = return a
runLocal (LApply _ f e) = do
  f' <- runLocal f
  e' <- runLocal e
  return $ f' e'

