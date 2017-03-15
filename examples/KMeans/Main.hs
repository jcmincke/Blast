{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns #-}


module Main where


--import Debug.Trace
import            Control.DeepSeq
import qualified  Data.List as L

import qualified  Data.Map.Strict as M

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)
import            Control.Distributed.Static (Closure)

import            System.Random

import            Control.Distributed.Blast
import            Control.Distributed.Blast.Runner.CloudHaskell as CH
import            Control.Distributed.Blast.Syntax

import            Common


type Point = (Double, Double)

dist :: forall a. Num a => (a, a) -> (a, a) -> a
dist (x1, y1) (x2, y2) = let
  dx = x2-x1
  dy = y2 -y1
  in dx * dx + dy * dy

p0 :: Point
p0 = (0, 0)

chooseCenter :: M.Map Point (Point, Int) -> Point -> M.Map Point (Point, Int)
chooseCenter centerAndSums p =
  r
  where
  !r = force $ M.insertWith (\((x0, y0), _) ((x, y), n) -> ((x0+x, y0+y), n+1)) bestCenter (p, 1) centerAndSums

  bestCenter = findCenter c d t
  (c:t) = M.keys centerAndSums
  d = dist c p
  findCenter currentCenter _ [] = currentCenter
  findCenter currentCenter currentDist (center:rest) = let
    d' = dist center p
    in  if d' < currentDist
        then findCenter center d' rest
        else findCenter currentCenter currentDist rest


computeNewCenters :: [M.Map Point (Point, Int)] -> M.Map Point Point
computeNewCenters l =
  y
  where
  l1 = do
    m <- l
    M.toList m
  x::M.Map Point [(Point, Int)]
  x = L.foldl' (\m (c, (p,n)) -> M.insertWith (++) c [(p, n)] m) M.empty l1
  y::M.Map Point Point
  y = M.map (\l' -> let (ps, ns) = L.unzip l'
                        (xs, ys) = L.unzip ps
                        sumX = sum xs
                        sumY = sum ys
                        n = sum ns
                        r = (sumX / (fromIntegral n), sumY / (fromIntegral n))
                        in r)
            x

deltaCenter :: M.Map Point Point -> Double
deltaCenter centers =
  r
  where
  r = maximum l
  l = L.map (\(p1, p2) -> sqrt $  dist p1 p2) $ M.toList centers

stop :: forall t t1 t2. Double -> (t1, Double) -> (t2, Double) -> t -> Bool
stop tol (_, x) (_, y::Double) _ = abs (x - y) < tol


-- example 1: based on a random generator, the cloud of points is generated on the master (no IO).

kmeansComputation1 :: StdGen -> Int -> ([Point], Double) -> LocalComputation (([Point], Double), [Point])
kmeansComputation1 stdGen nbPoints (centers, _) = do
      let vals = randomRs (0, 1) stdGen
      let points = L.take nbPoints $ makePoints vals
      rpoints <- rconst $ points
      centers0 <- lconst $ M.fromList $ L.map (\c -> (c, (p0, 0::Int))) centers
      centerMap <- rfold' (foldFun chooseCenter) computeNewCenters centers0 rpoints
      var' <- deltaCenter <$$> centerMap
      centers' <- M.elems <$$> centerMap
      r <- (,) <$$> centers' <**> var'
      (,) <$$> r <**> centers'
  where
  makePoints [] = []
  makePoints [_] = []
  makePoints (x:y:r) = (x,y):makePoints r


jobDesc1 :: JobDesc ([(Double, Double)], Double) [Point]
jobDesc1 =
  MkJobDesc (centers, 1000.0) (kmeansComputation1 stdGen nbPoints) noReporting (stop 0.1)
  where
  stdGen = mkStdGen 31415
  nbPoints = 100
  centers = [(0.0, 0.0), (1.0, 1.0)]


slaveClosure1 :: Int -> Process ()
slaveClosure1 = CH.slaveProcess rpcConfigAction jobDesc1




-- example 2: the cloud of points is generated on the master from a IO action
-- This simulates the case where the master reads some data (from a file, queue etc.) and sends it to the slaves.
kmeansComputation2 :: Int -> Int -> ([Point], Double) -> LocalComputation (([Point], Double), [Point])
kmeansComputation2 theSeed nbPoints (centers, _) = do
      rpoints <- rconstIO $ genPoints nbPoints
      centers0 <- lconst $ M.fromList $ L.map (\c -> (c, (p0, 0::Int))) centers
      centerMap <- rfold' (foldFun chooseCenter) computeNewCenters centers0 rpoints
      var' <- deltaCenter <$$> centerMap
      centers' <- M.elems <$$> centerMap
      r <- (,) <$$> centers' <**> var'
      (,) <$$> r <**> centers'
  where
  genPoints nbPoints' = do
    let stdGen = mkStdGen theSeed
    let vals = randomRs (0, 1) stdGen
    return $ L.take nbPoints' $ makePoints vals

  makePoints [] = []
  makePoints [_] = []
  makePoints (x:y:r) = (x,y):makePoints r


jobDesc2 :: JobDesc ([(Double, Double)], Double) [Point]
jobDesc2 =
  MkJobDesc (centers, 1000.0) (kmeansComputation2 theSeed nbPoints) noReporting (stop 0.1)
  where
  theSeed = 31415
  nbPoints = 100
  centers = [(0.0, 0.0), (1.0, 1.0)]


slaveClosure2 :: Int -> Process ()
slaveClosure2 = CH.slaveProcess rpcConfigAction jobDesc2





-- example 3: the cloud of points is generated on the slaves from a IO action
-- This simulates the case where each slave reads the data from a file, queue,...
-- Each slaves generates (around) nbPoints/nb-of-slaves.
kmeansComputation3 :: Int -> ([Point], Double) -> LocalComputation (([Point], Double), [Point])
kmeansComputation3 nbPoints (centers, _) = do
      range <- rconst $ Range 0 nbPoints
      rpoints <- rapply (funIO (\r -> do let n = L.length $ rangeToList r
                                         genPoints n)) range
      centers0 <- lconst $ M.fromList $ L.map (\c -> (c, (p0, 0::Int))) centers
      centerMap <- rfold' (foldFun chooseCenter) computeNewCenters centers0 rpoints
      var' <- deltaCenter <$$> centerMap
      centers' <- M.elems <$$> centerMap
      r <- (,) <$$> centers' <**> var'
      (,) <$$> r <**> centers'
  where
  genPoints nbPoints' = do
    stdGen <- newStdGen
    let vals = randomRs (0, 1) stdGen
    return $ L.take nbPoints' $ makePoints vals

  makePoints [] = []
  makePoints [_] = []
  makePoints (x:y:r) = (x,y):makePoints r




jobDesc3 :: JobDesc ([(Double, Double)], Double) [Point]
jobDesc3 =
  MkJobDesc (centers, 1000.0) (kmeansComputation3 nbPoints) noReporting (stop 0.1)
  where
  nbPoints = 100
  centers = [(0.0, 0.0), (1.0, 1.0)]


slaveClosure3 :: Int -> Process ()
slaveClosure3 = CH.slaveProcess rpcConfigAction jobDesc3

remotable ['slaveClosure1, 'slaveClosure2, 'slaveClosure3]

chClosure1 :: Int -> Closure (Process ())
chClosure1 = $(mkClosure 'slaveClosure1)

chClosure2 :: Int -> Closure (Process ())
chClosure2 = $(mkClosure 'slaveClosure2)

chClosure3 :: Int -> Closure (Process ())
chClosure3 = $(mkClosure 'slaveClosure3)

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable





-- main functions, choose the one you want to run.

mainCH1 :: IO ()
mainCH1 = runCloudHaskell rtable jobDesc1 chClosure1

mainLocal1 :: IO ()
mainLocal1 = do
  (a, r) <- runLocally True jobDesc1
  print a
  print r


mainCH2 :: IO ()
mainCH2 = runCloudHaskell rtable jobDesc2 chClosure2

mainLocal2 :: IO ()
mainLocal2 = do
  (a, r) <- runLocally True jobDesc2
  print a
  print r



mainCH3 :: IO ()
mainCH3 = runCloudHaskell rtable jobDesc3 chClosure3

mainLocal3 :: IO ()
mainLocal3 = do
  (a, r) <- runLocally True jobDesc3
  print a
  print r

{-
  Run Local:
    kmeans

  Run with CloudHaskell

    * start slaves:

        kmeans slave host port

    * start master:

        kmeans master host port

    ex:
      > kmeans slave localhost 5001
      > kmeans slave localhost 5002
      > kmeans master localhost 5000

-}

main :: IO ()
main = mainLocal1
