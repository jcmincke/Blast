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
import            Control.Monad.Logger

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)

import            Blast
import            Blast.Syntax
import qualified  Blast.Runner.Simple as S
import            Blast.Runner.CloudHaskell as CH


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

expGenerator :: Int -> ([Point], Double) -> LocalComputation (([Point], Double), [Point])
expGenerator nbPoints (centers, _) = do
      range <- rconst $ Range 0 nbPoints
      centers0 <- lconst $ M.fromList $ L.map (\c -> (c, (p0, 0::Int))) centers
      points <- rapply (fun(\r -> L.map (\i -> ((fromIntegral i) / fromIntegral nbPoints , (fromIntegral i) / fromIntegral nbPoints)) $ rangeToList r)) range

      centerMap <- rfold' (foldFun chooseCenter) computeNewCenters centers0 points
      var' <- deltaCenter <$$> centerMap
      centers' <- M.elems <$$> centerMap
      r <- (,) <$$> centers' <**> var'
      (,) <$$> r <**> centers'


criterion :: forall t t1 t2. Double -> (t1, Double) -> (t2, Double) -> t -> Bool
criterion tol (_, x) (_, y::Double) _ = abs (x - y) < tol

jobDesc :: JobDesc ([(Double, Double)], Double) [Point]
jobDesc = MkJobDesc ([(0.0, 0.0), (1.0, 1.0)], 1000.0) (expGenerator 100) reporting (criterion 0.1)


--rloc :: Bool -> IO ()
--rloc statefull = do
--  let cf = defaultConfig { statefullSlaves = statefull }
--  (a,b) <- logger $ Loc.runRec 1 cf jobDesc
--  print a
--  print b
--  return ()
--  where
--  logger a = runLoggingT a (\_ _ _ _ -> return ())


reporting :: forall t b. b -> t -> IO b
reporting a _ = do
  putStrLn "Reporting"
  putStrLn "End Reporting"
  return a




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

ch :: IO ()
ch = do
  args <- getArgs
  rpcConfig <- rpcConfigAction
  CH.runRec rtable rpcConfig args jobDesc $(mkClosure 'slaveClosure) k
  where
  k _ _ = do
    print "=========="

main :: IO ()
main = ch

simple :: IO ()
simple = do
  (a,b) <- logger $ S.runRec jobDesc
  print a
  print b
  return ()
  where
  logger a = runLoggingT a (\_ _ _ _ -> return ())
