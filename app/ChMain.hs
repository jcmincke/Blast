{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}



module Main where

import            Data.Proxy
import            Control.Monad.Operational
import            Control.Monad.Logger

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import            Blast.Syntax
import qualified  Blast.Runner.Simple as S
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH


c10 :: RemoteComputation [Int]
c10 = do
    r1 <- rconst [ (2::Int)| _ <- [1..10::Int]]
    r2 <- rmap (fun ((+) 1)) r1
    return r2

c11 :: RemoteComputation [Int] -> Int -> LocalComputation (Int, Int)
c11 c a = do
      r2 <- c
      zero <- lconst (0::Int)
      c1 <- lconst (0 ::Int)
      a2 <- rfold' (foldClosure c1 (const (+))) sum zero r2
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a2)
      return r

rr :: Int ->  LocalComputation (Int, Int)
rr = c11 c10

expg :: LocalComputation a -> () -> LocalComputation ((), a)
expg c1 () = do
  r <- c1
  (\v -> ((),v)) <$$> r


demo :: LocalComputation Int
demo = do
  a <- rconst [1..100]
  b <- rmap (fun (\x -> x * 2)) a
  lb <- collect b
  s <- sum <$$> lb
  c <- rmap (closure s (\y x -> x + y)) b
  lc <- collect c
  r <- sum <$$> lc
  return r




fib :: Int -> Int
fib 0 = 0
fib 1 = 1
fib 2 = 3
fib n = fib (n-1) + fib (n-2)

r1c :: RemoteComputation [Int]
r1c = rconst [ (2::Int)| _ <- [1..10::Int]]

expGenerator :: Int -> LocalComputation (Int, Int)
expGenerator (a::Int) = do
      r1 <- r1c
      r2 <- rmap (fun fib) r1
      zero <- lconst (0::Int)
      c1 <- lconst (0 ::Int)
      a2 <- rfold' (foldClosure c1 (const (+))) sum zero r2
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a2)
      return r

expGenerator3 :: Int -> LocalComputation(Int, [KeyedVal Int Int])
expGenerator3 (a::Int) = do
      r1 <- rconst [KeyedVal i (i*2) | i <- [1..10::Int]]

      a1 <- collect r1

      a' <- lconst (a+1)
      rf <- ((,) <$$> a' <**> a1)
      return rf



expGenerator2 :: Int -> LocalComputation (Int, [KeyedVal Int (Int, Int)])
expGenerator2 (a::Int) = do
      r1 <- rconst [KeyedVal i (i*2) | i <- [1..10::Int]]
      r2 <- rconst [KeyedVal i (i*3) | i <- [1..10::Int]]

      j1 <- rKeyedJoin (Proxy::Proxy []) r1 r2
      a1 <- collect j1


      a' <- lconst (a+1)
      rf <- ((,) <$$> a' <**> a1)
      return rf

expGenerator4 :: (Monad m, Builder m e) =>
  Int -> ProgramT (Syntax m) m (e 'Local (Int, [Int]))
expGenerator4 (a::Int) = do
      r1 <- rconst [ i| i <- [1..10::Int]]
      r2 <- rmap (fun ((+) 2)) r1
      r3 <- rmap (fun ((+) 3)) r2
      r4 <- rmap (fun ((+) 5)) r3
      a0 <- collect r4
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a0)
      return r

reporting :: forall t b. b -> t -> IO b
reporting a _ = do
  putStrLn "Reporting"
  --print a
  --print b
  putStrLn "End Reporting"
  return a

jobDesc :: JobDesc Int [Int]
jobDesc = MkJobDesc 0 expGenerator4 reporting (\_ _ _  -> True)

rloc :: Bool -> IO ()
rloc statefull = do
  let cf = defaultConfig { statefullSlaves = statefull }
  (a,b) <- runStdoutLoggingT $ Loc.runRec 1 cf jobDesc
  print a
  print b


simple :: IO ()
simple = do
  (a,b) <- runStdoutLoggingT $ S.runRec jobDesc
  print a
  print b

rpcConfigAction :: IO RpcConfig
rpcConfigAction = return $
  MkRpcConfig
    defaultConfig
    (MkMasterConfig runStdoutLoggingT)
    (MkSlaveConfig runStdoutLoggingT)

slaveClosure :: Int -> Process ()
slaveClosure = CH.slaveProcess rpcConfigAction jobDesc

remotable ['slaveClosure]

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable

main :: IO()
main = do
  args <- getArgs
  rpcConfig <- rpcConfigAction
  CH.runRec rtable rpcConfig args jobDesc $(mkClosure 'slaveClosure) k
  where
  k a b = do
    print a
    print b
    print "=========="

