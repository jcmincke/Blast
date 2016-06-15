{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Main where

import Debug.Trace
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Proxy
import            Control.Monad.IO.Class
import            Control.Monad.Operational
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Map as M
import            Data.Traversable
--import qualified  Data.Vault.Strict as V

import qualified  Data.Vector as V

import            Control.Distributed.Process (RemoteTable)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import qualified  Blast.Runner.Simple as S
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH


main = return ()

type Point = (Double, Double)

dist (x1, y1) (x2, y2) = let
  dx = x2-x1
  dy = y2 -y1
  in dx * dx + dy * dy

p0 :: Point
p0 = (0, 0)

chooseCenter :: M.Map Point (Point, Int) -> Point -> M.Map Point (Point, Int)
chooseCenter centerAndSums p =
  trace (show r) r
  where
  r = M.insertWith (\((x0, y0), _) ((x, y), n) -> ((x0+x, y0+y), n+1)) bestCenter (p, 1) centerAndSums

  bestCenter = findCenter c d t
  (c:t) = M.keys centerAndSums
  d = dist c p
  findCenter currentCenter currentDist [] = currentCenter
  findCenter currentCenter currentDist (center:t) = let
    d = dist center p
    in  if d < currentDist
        then findCenter center d t
        else findCenter currentCenter currentDist t

assignPoints :: V.Vector Point -> [Point] -> Range -> [(Point, (Point, Int))]
assignPoints points centers range =
  M.toList icenters'
  where
  icenters' = L.foldl' chooseCenter icenters $ L.map (\i -> points V.! i) $ rangeToList range
  icenters = M.fromList $ L.map (\c -> (c, (p0, 0))) centers

computeNewCenters :: [(Point, (Point, Int))] -> M.Map Point Point
computeNewCenters l =
  y
  where
  x::M.Map Point [(Point, Int)]
  x = L.foldl' (\m (c, (p,n)) -> M.insertWith (++) c [(p, n)] m) M.empty  l
  y::M.Map Point Point
  y = M.map (\l -> let (ps, ns) = L.unzip l
                       (xs, ys) = L.unzip ps
                       sumX = sum xs
                       sumY = sum ys
                       n = sum ns
                       r = (sumX / (fromIntegral n), sumY / (fromIntegral n))
                       in trace (show (xs, sumX, sumY, n, r, ns)) r)
            x

deltaCenter :: M.Map Point Point -> Double
deltaCenter centers =
  trace (show ("max = ", r)) r
  where
  r = maximum l
  l = L.map (\(p1, p2) -> sqrt $  dist p1 p2) $ M.toList centers

expGenerator :: V.Vector Point -> Computation ([Point], Double) [Point]
expGenerator points (centers, var) = do
      let nbPoints = V.length points
      range <- rconst $ Range 0 nbPoints
      ricenters <- rapply' (fun (assignPoints points centers)) range
      icenters <- collect ricenters
      centerMap <- computeNewCenters <$$> icenters
      var' <-  deltaCenter <$$> centerMap
      centers' <- M.elems <$$> centerMap
      r <- (,) <$$>  centers' <**> var'
      (,) <$$> r <**> centers'


points =
  V.fromList $ L.map (\i -> ((fromIntegral i) / fromIntegral n , (fromIntegral i) / fromIntegral n)) [1..n]
  where n = 1000
criterion tol (_, x) (_, y) _ = trace (show (x,y)) $ abs (x - y) < tol

jobDesc :: JobDesc ([Point], Double) [Point]
jobDesc = MkJobDesc ([(0, 0), (1, 1)], 1000.0) (expGenerator points) reporting (criterion 0.1)


rloc = do
  let cf = MkConfig True 1.0
  s <- logger $ Loc.createController cf 4 jobDesc
  (a,b) <- logger $ Loc.runRec cf s jobDesc
  print a
  print b
  where
  logger a = runLoggingT a (\_ _ _ _ -> return ())



reporting a b = do
  putStrLn "Reporting"
  --print a
  --print b
  putStrLn "End Reporting"
  return a



{-}
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

--main = return ()

-}