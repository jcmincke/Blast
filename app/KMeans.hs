{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns #-}


module Main where

import Debug.Trace
import            Control.Concurrent
import            Control.DeepSeq
import qualified  Data.List as L

import qualified  Data.Map.Strict as M
import            Data.Proxy
import            Control.Monad.IO.Class
import            Control.Monad.Operational
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import            Data.Traversable

import qualified  Data.Vector as V

import            Control.Distributed.Process (RemoteTable)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import            Blast.Syntax
import qualified  Blast.Runner.Simple as S
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH



type Point = (Double, Double)

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
  findCenter currentCenter currentDist [] = currentCenter
  findCenter currentCenter currentDist (center:t) = let
    d = dist center p
    in  if d < currentDist
        then findCenter center d t
        else findCenter currentCenter currentDist t

assignPoints :: Int -> [Point] -> Range -> [(Point, (Point, Int))]
assignPoints nbPoints centers range =
  r
  where
  !r = force $ M.toList icenters'
  icenters' = L.foldl' chooseCenter icenters $ points
  icenters = M.fromList $ L.map (\c -> (c, (p0, 0))) centers
  is = rangeToList range
  points = L.map (\i -> ((fromIntegral i) / fromIntegral nbPoints , (fromIntegral i) / fromIntegral nbPoints)) is


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
                       in r)
            x

deltaCenter :: M.Map Point Point -> Double
deltaCenter centers =
  r
  where
  r = maximum l
  l = L.map (\(p1, p2) -> sqrt $  dist p1 p2) $ M.toList centers

expGenerator :: Int -> ([Point], Double) -> LocalComputation (([Point], Double), [Point])
expGenerator nbPoints (centers, var) = do
--      let nbPoints = V.length points
      range <- rconst $ Range 0 nbPoints
      ricenters <- rapply (funIO (proc nbPoints centers)) range
      icenters <- collect ricenters
      centerMap <- computeNewCenters <$$> icenters
      var' <-  deltaCenter <$$> centerMap
      centers' <- M.elems <$$> centerMap
      r <- (,) <$$>  centers' <**> var'
      (,) <$$> r <**> centers'
      where
      proc nbPoints centers range = do
        putStrLn "start"
        let !r = (assignPoints nbPoints centers range)
        putStrLn "end"
        return r


criterion tol (_, x) (_, y) _ = abs (x - y) < tol

jobDesc = MkJobDesc ([(0, 0), (1, 1)], 1000.0) (expGenerator 1000) reporting (criterion 0.1)


rloc statefull = do
  let cf = defaultConfig { statefullSlaves = statefull }
  (a,b) <- logger $ Loc.runRec 4 cf jobDesc
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


rpcConfigAction = return $
  MkRpcConfig
    (defaultConfig { shouldOptimize = False })
    (MkMasterConfig runStdoutLoggingT)
    (MkSlaveConfig runStdoutLoggingT)


slaveClosure = CH.slaveProcess rpcConfigAction jobDesc

remotable ['slaveClosure]

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable


ch = do
  args <- getArgs
  rpcConfig <- rpcConfigAction
  CH.runRec rtable rpcConfig args jobDesc $(mkClosure 'slaveClosure) k
  where
  k a b = do
    print a
    print b
    print "=========="


main = ch
simple = do
  (a,b) <- logger $ S.runRec False jobDesc
  print a
  print b
  where
  logger a = runLoggingT a (\_ _ _ _ -> return ())
