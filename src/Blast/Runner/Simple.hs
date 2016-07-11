{-|
Module      : Blast.Syntax
Copyright   : (c) Jean-Christophe Mincke, 2016
License     : BSD3
Maintainer  : jeanchristophe.mincke@gmail.com
Stability   : experimental
Portability : POSIX

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

import            Blast.Types

data Exp (k::Kind) a where
  RApply :: Int -> ExpClosure Exp a b -> Exp 'Remote a -> Exp 'Remote b
  RConst :: Int -> a -> Exp 'Remote a
  LConst :: Int -> a -> Exp 'Local a
  Collect :: Int -> Exp 'Remote a -> Exp 'Local a
  LApply :: Int -> Exp 'Local (a -> b) -> Exp 'Local a -> Exp 'Local b


instance (Monad m) => Builder m Exp where
  makeRApply n f a = do
    return $ RApply n f a
  makeRConst n a = do
    return $ RConst n a
  makeLConst n a = do
    return $ LConst n a
  makeCollect n a = do
    return $ Collect n a
  makeLApply n f a = do
    return $ LApply n f a
  fuse refMap n e = return (e, refMap, n)



instance Indexable Exp where
  getIndex (RApply n _ _) = n
  getIndex (RConst n _) = n
  getIndex (LConst n _) = n
  getIndex (Collect n _) = n
  getIndex (LApply n _ _) = n





-- | Runs a computation using a simple interpreter. Execute all computations on just one thread.
runRec :: forall a b m. (MonadLoggerIO m) =>
  Bool
  -> JobDesc a b
  -> m (a, b)
runRec shouldOptimize (jobDesc@MkJobDesc {..}) = do
  let program = computationGen seed
  (refMap, count) <- generateReferenceMap 0 M.empty program
  !(e::Exp 'Local (a,b)) <- build shouldOptimize refMap (0::Int) count program
  (a,b) <- liftIO $ runLocal e
  a' <- liftIO $ reportingAction a b
  case recPredicate a a' b of
    True -> do
      return (a', b)
    False -> do
      runRec shouldOptimize (jobDesc {seed = a'})



runFun :: ExpClosure Exp a b -> IO (a -> IO b)
runFun (ExpClosure e f) = do
  r <- runLocal e
  return $ f r



runRemote :: Exp 'Remote a -> IO a
runRemote (RApply _ cs e) = do
  f' <- runFun cs
  e' <- runRemote e
  trace "RApply" $ f' e'

runRemote (RConst _ e) = return e

runLocal ::  Exp 'Local a -> IO a
runLocal (Collect _ e) = runRemote e
runLocal (LConst _ a) = return a
runLocal (LApply _ f e) = do
  f' <- runLocal f
  e' <- runLocal e
  return $ f' e'

