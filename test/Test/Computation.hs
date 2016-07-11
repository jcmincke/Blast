{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Test.Computation
where

import Debug.Trace
--import            Control.Applicative
import            Control.Monad.Logger
import            Control.Monad.IO.Class
import qualified  Data.List as L
import            Data.Proxy
import qualified  Data.Vector as V

import            Test.HUnit
import            Test.Framework
import            Test.Framework.Providers.HUnit
import            Test.Framework.Providers.QuickCheck2 (testProperty)
import            Test.QuickCheck
import            Test.QuickCheck.Arbitrary
import            Test.QuickCheck.Monadic
import            Test.QuickCheck.Random
import            Test.QuickCheck.Gen

import            Blast as S
import            Blast.Runner.Local as Loc
import            Blast.Runner.Simple as S
import            Blast.Syntax as S

import            Blast.Distributed.Interface as I



generateOne :: Int -> Gen a -> a
generateOne seed (MkGen g) =
  g qCGen 30
  where
  qCGen = mkQCGen seed

tests = [
   testProperty "testComputation" testComputation
    ]



testComputation :: Property
testComputation  =
  monadicIO $ forAllM arbitrary prop




prop :: Int -> PropertyM IO Bool
prop seed = do
  (a1, b1::Int) <- liftIO $ prop1 (jobDescFun ())
  (a2, b2::Int) <- liftIO $ prop2 (jobDescFun ())
  return $ (a1, b1) == (a2, b2)
  where
  depth = 10
  jobDescFun () =
    let proc a = do
          let computation = generateOne seed (compGen depth)

          b <- computation
          a' <- lconst (a+1)
          r <- ((,) <$$> a' <**> b)
          return r
        jobDesc = MkJobDesc (0::Int)
                    proc
                    (\a _ -> return a)
                    (\_ x _  -> x==1)
    in jobDesc


prop1 :: JobDesc Int Int -> IO (Int, Int)
prop1 jobDesc = do
    (a1, b1::Int) <- runStdoutLoggingT $ S.runRec False jobDesc
    return (a1, b1)


prop2 :: JobDesc Int Int -> IO (Int, Int)
prop2 jobDesc = do
    let cf = defaultConfig { statefullSlaves = True, shouldOptimize = False }
    (a2, b2::Int) <- runStdoutLoggingT $ Loc.runRec 1 cf jobDesc
    return (a2, b2)


go :: forall m e. (Monad m, Builder m e) => Int -> [Computation m e 'Remote [Int]] -> [Computation m e 'Local Int] -> Gen (Computation m e 'Local Int)
go 0 _ locals = elements locals
go n remotes locals = do
    b <- arbitrary
    case b of
      True -> do
        remotes' <- addRemote remotes locals
        go (n-1) remotes' locals
      False -> do
        locals' <- addLocal remotes locals
        go (n-1) remotes locals'

compGen :: forall m e. (Monad m, Builder m e) => Int -> Gen (Computation m e 'Local Int)
compGen n = do
  let remotes0 = [rconst []]
  let locals0 = [lconst (0::Int)]
  computation <- go n remotes0 locals0
  return computation
  where


addRemote :: forall m e. (Monad m, Builder m e)
  => [Computation m e 'Remote [Int]]
  -> [Computation m e 'Local Int]
  -> Gen [Computation m e 'Remote [Int]]
addRemote remotes locals = do
  frequency [(1, gen1), (1, gen2)]
  where
  gen1 :: Gen [Computation m e 'Remote [Int]]
  gen1 = do
    local <- elements locals
    remote <- elements remotes
    let (c :: Computation m e 'Remote [Int]) = do
                r <- remote
                a <- local
                rmap (closure a (\a' e  -> e+a')) r
    return (c:remotes)
  gen2 :: Gen [Computation m e 'Remote [Int]]
  gen2 = do
    n <- choose (10, 50)
    vals <- vectorOf n (choose (-1, 1))
    let (c :: Computation m e 'Remote [Int]) = rconst vals
    return (c:remotes)




addLocal ::forall m e. (Monad m, Builder m e)
  => [Computation m e 'Remote [Int]]
  -> [Computation m e 'Local Int]
  -> Gen [Computation m e 'Local Int]
addLocal remotes locals = do
  frequency [(1, gen1), (1, gen2), (1, gen3)]
  gen3
  where
  gen1 = do
    remote <- elements remotes
    let c = do
              r <- remote
              a <- S.collect r
              zero <- lconst (0::Int)
              (L.foldl' (+)) <$$> zero <**> a
    return (c:locals)
  gen2 = do
    v <- choose (-1, 1)
    let c = lconst v
    return (c:locals)
  gen3 = do
    l1 <- elements locals
    l2 <- elements locals
    let c = do
          a1 <- l1
          a2 <- l2
          (+) <$$> a1 <**> a2
    return (c:locals)


