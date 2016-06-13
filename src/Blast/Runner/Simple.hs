{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Runner.Simple
(
  runRec
)
where

import            Data.Functor.Identity
import            Control.Monad.IO.Class
import            Control.Monad.Logger

import            Blast.Types

data Exp (k::Kind) a where
  RApply :: ExpClosure Exp a b -> Exp 'Remote a -> Exp 'Remote b
  RConst ::  a -> Exp 'Remote a
  LConst :: a -> Exp 'Local a
  Collect :: Exp 'Remote a -> Exp 'Local a
  LApply :: Exp 'Local (a -> b) -> Exp 'Local a -> Exp 'Local b


instance Builder Identity Exp where
  makeRApply f a = do
    return $ RApply f a
  makeRConst a = do
    return $ RConst a
  makeLConst a = do
    return $ LConst a
  makeCollect a = do
    return $ Collect a
  makeLApply f a = do
    return $ LApply f a


runRec :: forall a b m.(Builder m Exp, MonadLoggerIO m) => JobDesc a b -> m (a, b)
runRec (jobDesc@MkJobDesc {..}) = do
  (e::Exp 'Local (a,b)) <- build (expGen seed)
  (a,b) <- liftIO $ runLocal e
  a' <- liftIO $ reportingAction a b
  case recPredicate a' of
    True -> do
    --      $(logInfo) "Finished"
      return (a', b)
    False -> runRec (jobDesc {seed = a'})



runFun :: ExpClosure Exp a b -> IO (a -> IO b)
runFun (ExpClosure e f) = do
  r <- runLocal e
  return $ f r



runRemote :: Exp 'Remote a -> IO a
runRemote (RApply cs e) = do
  f' <- runFun cs
  e' <- runRemote e
  f' e'

runRemote (RConst e) = return e

runLocal ::  Exp 'Local a -> IO a
runLocal (Collect e) = runRemote e
runLocal (LConst a) = return a
runLocal (LApply f e) = do
  f' <- runLocal f
  e' <- runLocal e
  return $ f' e'

