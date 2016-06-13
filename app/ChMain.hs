{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Main where

import Debug.Trace
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Proxy
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import            Data.Traversable
import qualified  Data.Vault.Strict as V

import            Control.Distributed.Process (RemoteTable)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import qualified  Blast.Runner.Simple as S
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH


{-
expGenerator a = do
      r1 <- cstRdd [1..100000::Int]
      r2 <- smap r1 $ fun ((+) a)
      zero <- cstLocal 0
      sum2 <- slocalfold r2 (foldFun (+)) zero
      r3 <- smap r1 (closure sum2 (\s a -> a+s))
      sum3 <- slocalfold r3 (foldFun (+)) zero
      r4 <- smap r2 (closure sum3 (\s a -> a+s))
      sum4 <- slocalfold r4 (foldFun (+)) zero
      a' <- cstLocal (a+1)
      r <- sfrom ((,) <$$> a' <**> sum4)
      return r
-}


fib :: Int -> Int
fib 0 = 0
fib 1 = 1
fib 2 = 3
fib n = fib (n-1) + fib (n-2)


expGenerator (a::Int) = do
      r1 <- rconst [ 2| _ <- [1..10::Int]]
      r2 <- rmap (fun fib) r1
      zero <- lconst (0::Int)
      c1 <- lconst (0 ::Int)
      a2 <- rfold' (foldClosure c1 (const (+))) sum zero r2
      --a2 <- collect r2
--      a2 <- slocalfold r1 (foldFun (+)) zero
      one <- lconst (1::Int)
      ar2 <- collect r2
      a3 <- lfold' (*) one ar2
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a2)
      --  liftIO $ print "hello"
      return r


expGenerator3 (a::Int) = do
      r1 <- rconst [KeyedVal i (i*2) | i <- [1..10::Int]]

      a1 <- collect r1

      a' <- lconst (a+1)
      rf <- ((,) <$$> a' <**> a1)
      return rf




expGenerator2 (a::Int) = do
      r1 <- rconst [KeyedVal i (i*2) | i <- [1..10::Int]]
      r2 <- rconst [KeyedVal i (i*3) | i <- [1..10::Int]]

      j1 <- rKeyedJoin (Proxy::Proxy []) r1 r2
      a1 <- collect j1

  --    j2 <- rKeyedJoin r1 r2
 --     a2 <- collect j2

      a' <- lconst (a+1)
--      r <- ((,) <$$> a1 <**> a2)
      rf <- ((,) <$$> a' <**> a1)
      return rf


reporting a b = do
  putStrLn "Reporting"
  --print a
  --print b
  putStrLn "End Reporting"
  return a

--jobDesc :: (MonadIO m) => JobDesc m Int Int
jobDesc = MkJobDesc 0 expGenerator3 reporting (\x -> True)


rloc = do
  let cf = MkConfig True 1.0
  s <- runStdoutLoggingT $ Loc.createController cf 1 jobDesc
  (a,b) <- runStdoutLoggingT $ Loc.runRec cf s jobDesc
  print a
  print b



rpcConfigAction = return $
  MkRpcConfig
    (MkConfig True 1.0)
    (MkMasterConfig runStdoutLoggingT)
    (MkSlaveConfig runStdoutLoggingT)


slaveClosure = CH.slaveProcess rpcConfigAction jobDesc

remotable ['slaveClosure]

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable


main = do
  args <- getArgs
  rpcConfig <- rpcConfigAction
  CH.runRec rtable rpcConfig args jobDesc $(mkClosure 'slaveClosure) k
  where
  k a b = do
    print a
    print b
    print "=========="


