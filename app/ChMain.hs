{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Main where

import Debug.Trace
import qualified  Data.List as L
import qualified  Data.Map as M
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Vault.Strict as V

import            Control.Distributed.Process (RemoteTable)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import            Blast.Distributed.Rpc.CloudHaskell



expGenerator a = do
      r1 <- cstRdd [1..10::Int]
      r2 <- smap r1 $ fun ((+) a)
      sum2 <- sfold r2 (+) 0
      r3 <- smap r1 (closure sum2 (\s a -> a+s))
      sum3 <- sfold r3 (+) 0
      r4 <- smap r2 (closure sum3 (\s a -> a+s))
      sum4 <- sfold r4 (+) 0
      a' <- cstLocal (a+1)
      r <- sfrom ((,) <$$> a' <**> sum4)
      return r


jobDesc = MkJobDesc True 0 expGenerator (\x -> x==10)

slaveClosure = slaveProcess jobDesc

remotable ['slaveClosure]

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable

main :: IO ()
main = do
  args <- getArgs
  runRpc rtable args jobDesc $(mkClosure 'slaveClosure) k
  where
  k a b = do
    print a
    print b