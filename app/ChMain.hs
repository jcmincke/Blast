{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}


module Main where

import Debug.Trace
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Proxy
import            Control.Monad.IO.Class
import            Control.Monad.Operational
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Serialize as S
import            Data.Traversable
import qualified  Data.Vault.Strict as V

import            Control.Distributed.Process (RemoteTable)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import            Blast.Syntax
import            Blast.Runner.Simple as BRS
import qualified  Blast.Runner.Simple as S
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH

--c1 :: (Monad m, Builder m e) =>
--   ProgramT (Syntax m) m (e 'Local [Int])
c1 :: LocalComputation [Int]
c1 = do
      r1 <- rconst [ (2::Int)| _ <- [1..10::Int]]
      a1 <- collect r1
      return a1

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
      one <- lconst (1::Int)
      ar2 <- collect r2
      x <- return 8
      a3 <- lfold' (*) one ar2
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a2)
      return r

rr :: Int ->  LocalComputation (Int, Int)
rr = c11 c10

--expg
--  :: (Monad m, Builder m e) =>
--     ProgramT (Syntax m) m (e 'Local a) -> () -> ProgramT (Syntax m) m (e 'Local ((), a))
expg :: LocalComputation a -> () -> LocalComputation ((), a)
expg c1 () = do
  r <- c1
  (\v -> ((),v)) <$$> r


--  computation = c1
{-

t :: (MonadLoggerIO m) =>  ProgramT (Syntax m) m (e 'Local [Int]) -> m [Int]
t computation =  do
  (_, r) <- BRS.runRec True jd
  return r
  where

jd :: JobDesc () [Int]
jd = MkJobDesc () (expg c1) (\() _ -> return ()) (\() () _ -> True)

jobDesc2 :: JobDesc () [Int]
jobDesc2 = MkJobDesc () (expg c1) (\_ _ -> return ()) (\_ _ _ -> True)

runOnce :: forall a e m.(Monad m, Builder m e, MonadLoggerIO m) =>
  Bool -> ProgramT (Syntax m) m (e 'Local a) ->  m () -- ProgramT (Syntax m) m (e 'Local ((), a))
runOnce shouldOptimize computation = do
  (_, r) <- BRS.runRec shouldOptimize jobDesc2
  return ()
  where
  --jobDesc2 :: JobDesc () a
  jobDesc2 = MkJobDesc () (expg computation) (\() _ -> return ()) (\() () _ -> True)


runOnce :: forall a e m.(Monad m, Builder m e, MonadLoggerIO m) => Bool -> ProgramT (Syntax m) m (e 'Local a) -> m ()
runOnce shouldOptimize computation = do
--  (_, r) <- runRec shouldOptimize jobDesc
  return ()
  where
  expGen :: () -> ProgramT (Syntax m) m (e 'Local ((), a))
  expGen () = do
    a <- computation
    (\v -> ((),v)) <$$> a
  jobDesc :: JobDesc () a
  jobDesc = MkJobDesc () expGen (\_ _ -> return ()) (\_ _ _ -> True)
expg () = do
  r <- c1
  (\v -> ((),v)) <$$> r

-}


{-


runOnce :: forall a e m.(Builder m e, MonadLoggerIO m) => Bool -> ProgramT (Syntax m) m (e 'Local a) -> m ()
runOnce shouldOptimize computation = do
--  (_, r) <- runRec shouldOptimize jobDesc
  return ()
  where
  expGen :: () -> ProgramT (Syntax m) m (e 'Local ((), a))
  expGen () = do
    a <- computation
    (\v -> ((),v)) <$$> a
  jobDesc :: JobDesc () a
  jobDesc = MkJobDesc () expGen (\_ _ -> return ()) (\_ _ _ -> True)

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

r1c :: RemoteComputation [Int]
r1c = rconst [ (2::Int)| _ <- [1..10::Int]]

--expGenerator :: Int -> Computation Int Int
expGenerator (a::Int) = do
      r1 <- r1c
      r2 <- rmap (fun fib) r1
      zero <- lconst (0::Int)
      c1 <- lconst (0 ::Int)
      a2 <- rfold' (foldClosure c1 (const (+))) sum zero r2
      one <- lconst (1::Int)
      ar2 <- collect r2
      x <- return 8
      a3 <- lfold' (*) one ar2
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a2)
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

expGenerator4 (a::Int) = do
      r1 <- rconst [ i| i <- [1..10::Int]]
      r2 <- rmap (fun ((+) 2)) r1
      r3 <- rmap (fun ((+) 3)) r2
      r4 <- rmap (fun ((+) 5)) r3
      a0 <- collect r4
      a' <- lconst (a+1)
      r <- ((,) <$$> a' <**> a0)
      return r


reporting a b = do
  putStrLn "Reporting"
  --print a
  --print b
  putStrLn "End Reporting"
  return a

jobDesc = MkJobDesc 0 expGenerator4 reporting (\_ x _  -> True)


rloc statefull = do
  let cf = defaultConfig { statefullSlaves = statefull, shouldOptimize = False }
  (a,b) <- runStdoutLoggingT $ Loc.runRec 1 cf jobDesc
  print a
  print b


simple :: IO ()
simple = do
  (a,b) <- runStdoutLoggingT $ S.runRec False jobDesc
  print a
  print b

rpcConfigAction = return $
  MkRpcConfig
    (defaultConfig {shouldOptimize = False})
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

--main = return ()

