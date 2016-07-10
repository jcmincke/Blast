{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

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

import            Blast as S
import            Blast.Syntax as S
import            Blast.Runner.Simple as S

instance Show (a->b) where
  show _ = "a fun"

instance Show (JobDesc a b) where
  show _ = "job desc"

tests = [
   testProperty "testComputation" testComputation
    ]

testComputation :: Property
testComputation  =
--  monadicIO $ forAllM (choose (0::Int, 1)) prop
  monadicIO $ forAllM gen1 prop
  where
  gen1 = compGen 1
  prop jobDesc = do
    (a,b::Int) <- liftIO $ runStdoutLoggingT $ S.runRec False jobDesc
    liftIO $ print "dd"
    return True


{-}
simple :: IO ()
simple = do
  (a,b) <- runStdoutLoggingT $ S.runRec False jobDesc
  print a
  print b

testComputation =
  monadicIO $ forAllM (compGen 1) prop
  where
  prop ::  (Int -> Computation m e 'Local (Int, Int)) -> IO Bool
  prop computation = do
    (a,b) <- runStdoutLoggingT $ S.runRec False jobDesc
    return True
    where
    jobDesc = MkJobDesc 0
                computation
                (\a _ -> return a)
                (\_ x _  -> x==1)
-}



compGen :: forall m e. (Monad m, Builder m e) => Int -> Gen (JobDesc Int Int)
compGen n = do
  let remotes0 = [rconst []]
  let locals0 = [lconst (0::Int)]
  (computation :: (Computation m e 'Local Int)) <- go n remotes0 locals0

  let (proc :: Int -> (Computation m e 'Local (Int, Int))) = \a -> do
          b <- computation
          a' <- lconst (a+1)
          r <- ((,) <$$> a' <**> b)
          return r
  let jobDesc = MkJobDesc (0::Int)
                  proc
                  (\a _ -> return a)
                  (\_ x _  -> x==1)
  return jobDesc
  where

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



addRemote :: forall m e. (Monad m, Builder m e) => [Computation m e 'Remote [Int]] -> [Computation m e 'Local Int] -> Gen [Computation m e 'Remote [Int]]
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

addLocal ::forall m e. (Monad m, Builder m e) => [Computation m e 'Remote [Int]] -> [Computation m e 'Local Int] -> Gen [Computation m e 'Local Int]
addLocal remotes locals = do
  frequency [(1, gen1), (1, gen2), (1, gen3)]
  where
  gen1 = do
    remote <- elements remotes
    let (c::Computation m e 'Local Int) = do
              r <- remote
              a <- S.collect r
              zero <- lconst (0::Int)
              (L.foldl' (+)) <$$> zero <**> a
    return (c:locals)
  gen2 = do
    v <- choose (-1, 1)
    let (c:: Computation m e 'Local Int) = lconst v
    return (c:locals)
  gen3 = do
    l1 <- elements locals
    l2 <- elements locals
    let (c:: Computation m e 'Local Int) = do
          a1 <- l1
          a2 <- l2
          (+) <$$> a1 <**> a2
    return (c:locals)

