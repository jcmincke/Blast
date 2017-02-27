{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}



module Main where

import qualified  Data.List as L
import            Data.Map as M

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)
import            Control.Distributed.Static (Closure)

import            Blast
import            Blast.Runner.CloudHaskell as CH
import            Blast.Syntax

import            Common


-- Example 1: Simple mapping. Sum of fibonacci numbers

comp1 :: () -> LocalComputation ((), Int)
comp1 () = do
      -- create a remote list [32, 32, 32 ,...]
      r1 <- rconst [ (31::Int)| _ <- [1..32::Int]]
      -- map fib over the remote list r1
      r2 <- rmap (fun fib) r1
      -- repatriate the results locally.
      l1 <- collect r2
      -- sum them
      l2 <- sum <$$> l1
      -- associate new seed
      r <- (\x -> ((), x)) <$$> l2
      return r

-- create the job, no intermediate reporting, no iteration.

jobDesc1 :: JobDesc () Int
jobDesc1 = MkJobDesc () comp1 noReporting noIteration

slaveClosure1 :: Int -> Process ()
slaveClosure1 = CH.slaveProcess rpcConfigAction jobDesc1




-- Example 2: Simple optimized folding on both slaves and master. Sum of fibonacci numbers

comp2 :: () -> LocalComputation ((), Int)
comp2 () = do
      -- create a remote list [32, 32, 32 ,...]
      r1 <- rconst [ (31::Int)| _ <- [1..32::Int]]
      -- fold over the remote list r1
      zero <- lconst (0::Int)
      l1 <- rfold' (foldFun (\acc i -> acc + fib i)) sum zero r1
      -- associate new seed
      r <- (\x -> ((), x)) <$$> l1
      return r


-- create the job, no intermediate reporting, no iteration.

jobDesc2 :: JobDesc () Int
jobDesc2 = MkJobDesc () comp2 noReporting noIteration

slaveClosure2 :: Int -> Process ()
slaveClosure2 = CH.slaveProcess rpcConfigAction jobDesc2




-- Example 3: Simple iterative process.
strangeConjecture :: Int -> Int
strangeConjecture 1 = 1
strangeConjecture n | even n = n `div` 2
strangeConjecture n = 3 * n + 1

comp3 :: [Int] -> LocalComputation ([Int], Int)
comp3 ints = do
      r1 <- rconst ints
      r2 <- rmap (fun strangeConjecture) r1
      l1 <- collect r2
      l2 <- removeOnes <$$> l1
      lc <- L.length <$$> l2
      r <- (,) <$$> l2 <**> lc
      return r
  where
  removeOnes l = L.filter (\x -> x > 1) l

-- create the job, iterate until the result list is empty

jobDesc3 :: JobDesc [Int] Int
jobDesc3 =
  MkJobDesc ints comp3 report stop
  where
  ints = [1..1000]
  report a c = do
    putStrLn $ "Nb of remaining ints: " ++ show c
    return a
  stop _ _ c = c==0


slaveClosure3 :: Int -> Process ()
slaveClosure3 = CH.slaveProcess rpcConfigAction jobDesc3

-- Example 4: Elaborate on example 3:
-- Compute the number of calls to strange required to reach 1.

comp4 :: ([(Int, Int, Int)], Map Int Int) ->  LocalComputation (([(Int, Int, Int)], Map Int Int), ())
comp4 (values, m) = do
      r1 <- rconst values
      r2 <- rmap (fun mapStrange) r1
      l1 <- collect r2
      l2 <- (removeOnes m) <$$> l1
      r <- (\x -> (x, ())) <$$> l2
      return r
  where
  mapStrange (i, n, c) = (i, strangeConjecture n, c+1)
  removeOnes :: (M.Map Int Int) -> [(Int, Int, Int)] -> ([(Int, Int, Int)], M.Map Int Int)
  removeOnes m' l =
    L.foldl' proc ([], m') l
    where
    proc (acc, m'') e@(i, n, c) =
      if n > 1
      then (e:acc, m'')
      else (acc, M.insert i c m'')

-- create the job, iterate until the result list is empty

jobDesc4 :: JobDesc ([(Int, Int, Int)], (M.Map Int Int)) ()
jobDesc4 =
  MkJobDesc (values, M.empty) comp4 report stop
  where
  values = [(i, i, 0) | i <- [1..1000]]
  report a@(values', _) () = do
    putStrLn $ "Nb of remaining values: " ++ show (L.length values')
    return a
  stop _ (values', _) () = L.length values' == 0


slaveClosure4 :: Int -> Process ()
slaveClosure4 = CH.slaveProcess rpcConfigAction jobDesc4



-- Example 5: Simple closure.

comp5 :: () -> LocalComputation ((), Int)
comp5 () = do
      r1 <- rconst [1..1000::Int]
      lc1 <- lconst 2
      r2 <- rmap (closure lc1 (\c a -> a*c)) r1
      l1 <- collect r2
      l2 <- sum <$$> l1
      r <- (\x -> ((), x)) <$$> l2
      return r

-- create the job, no intermediate reporting, no iteration.

jobDesc5 :: JobDesc () Int
jobDesc5 = MkJobDesc () comp5 noReporting noIteration

slaveClosure5 :: Int -> Process ()
slaveClosure5 = CH.slaveProcess rpcConfigAction jobDesc5





-- create remotables
remotable ['slaveClosure1, 'slaveClosure2, 'slaveClosure3, 'slaveClosure4, 'slaveClosure5]


chClosure1 :: Int -> Closure (Process ())
chClosure1 = $(mkClosure 'slaveClosure1)

chClosure2 :: Int -> Closure (Process ())
chClosure2 = $(mkClosure 'slaveClosure2)

chClosure3 :: Int -> Closure (Process ())
chClosure3 = $(mkClosure 'slaveClosure3)

chClosure4 :: Int -> Closure (Process ())
chClosure4 = $(mkClosure 'slaveClosure4)

chClosure5 :: Int -> Closure (Process ())
chClosure5 = $(mkClosure 'slaveClosure5)

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable




-- main functions, choose the one you want to run.

mainCH1 :: IO ()
mainCH1 = runCloudHaskell rtable jobDesc1 chClosure1

mainLocal1 :: IO ()
mainLocal1 = do
  (_, r) <- runLocally True jobDesc1
  print r


mainCH2 :: IO ()
mainCH2 = runCloudHaskell rtable jobDesc2 chClosure2

mainLocal2 :: IO ()
mainLocal2 = do
  (_, r) <- runLocally True jobDesc2
  print r


mainCH3 :: IO ()
mainCH3 = runCloudHaskell rtable jobDesc3 chClosure3

mainLocal3 :: IO ()
mainLocal3 = do
  (_, r) <- runLocally True jobDesc3
  print r


mainCH4 :: IO ()
mainCH4 = runCloudHaskell rtable jobDesc4 chClosure4

mainLocal4 :: IO ()
mainLocal4 = do
  (([], m), ()) <- runLocally True jobDesc4
  print m


mainCH5 :: IO ()
mainCH5 = runCloudHaskell rtable jobDesc4 chClosure4

mainLocal5 :: IO ()
mainLocal5 = do
  (_, r)  <- runLocally True jobDesc5
  print r



{-
  Run Local:
    simple

  Run with CloudHaskell

    * start slaves:

        simple slave host port

    * start master:

        simple master host port

    ex:
      > simple slave localhost 5001
      > simple slave localhost 5002
      > simple master localhost 5000

-}

-- main and rtable
main :: IO ()
main = mainLocal5


