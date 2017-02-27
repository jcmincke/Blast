{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}



module Common where

import            Control.Monad.Logger

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Static (Closure)

import            Data.Serialize (Serialize)
import            System.Environment (getArgs)

import            Blast
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH


-- a slow implementation of Fibonnacci
fib :: Int -> Int
fib 0 = 0
fib 1 = 1
fib 2 = 3
fib n = fib (n-1) + fib (n-2)

noReporting :: a -> b -> IO a
noReporting a _ = return a

noIteration :: a -> a -> b -> Bool
noIteration _ _ _ = True



runLocally :: (Serialize b, Serialize a) =>
  Bool -> JobDesc a b -> IO (a, b)
runLocally statefull jobDesc = do
  let cf = defaultConfig { statefullSlaves = statefull }
  let nbSlaves = 8
  runStdoutLoggingT $ Loc.runRec nbSlaves cf jobDesc


rpcConfigAction :: IO RpcConfig
rpcConfigAction = return $
  MkRpcConfig
    defaultConfig
    (MkMasterConfig runStdoutLoggingT)
    (MkSlaveConfig runStdoutLoggingT)


runCloudHaskell :: (Show a, Show b, Serialize a, Serialize b) =>
  RemoteTable
  -> JobDesc a b
  -> (Int -> Closure (Process ()))
  -> IO ()
runCloudHaskell rtable jobDesc chClosure = do
  args <- getArgs
  rpcConfig <- rpcConfigAction
  CH.runRec rtable rpcConfig args jobDesc chClosure k
  where
  k a b = do
    print a
    print b
    print "End"

