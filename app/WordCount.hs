{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns #-}


module Main where

--import Debug.Trace
import qualified  Data.List as L

import qualified  Data.Map.Strict as M
import            Control.Monad.Logger

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)

import            System.Environment (getArgs)
import            Data.Conduit
import            Data.Conduit.List as CL
import            Data.Conduit.Binary as CB
import            Control.Monad.Trans.Resource
import            Blast
import            Blast.Syntax
import            Blast.Runner.Local as Loc
import            Blast.Runner.CloudHaskell as CH

import qualified Data.ByteString as BS
import Data.Word



--sa :: MonadResource m => Conduit i m BS.ByteString --ConduitM i ByteString m ()
--sa = sourceFile "./files/wordcount/f1.txt"
--
--sb :: MonadResource m => Conduit BS.ByteString m o -- ConduitM ByteString o m ()
--sb = sinkFile "./files/wordcount/f1-2.txt"
--
--sc:: MonadResource m => Conduit i m BS.ByteString -> Conduit i m BS.ByteString
--sc  a = mapOutput (\bs ->
--              let l = L.reverse $ BS.unpack bs
--              in  BS.pack l) a
--
--sd::MonadResource m => Conduit i m BS.ByteString
--sd = sc sa

countChar :: M.Map Word8 Int -> BS.ByteString -> M.Map Word8 Int
countChar m bs =
  BS.foldl' foldProc m bs
  where
  foldProc m' c = M.insertWith (+) c 1 m'

--createSource :: MonadResource m => FilePath -> ConduitM i BS.ByteString m ()
--createSource f = sourceFile f

reduceCharMap :: M.Map Word8 Int -> M.Map Word8 Int -> M.Map Word8 Int
reduceCharMap acc m = M.unionWith (+) acc m

expGenerator :: Int -> () -> LocalComputation ((), M.Map Word8 Int)
expGenerator nbFiles () = do
      range <- rconst $ Range 1 (nbFiles+1)
      indexes <- rapply (fun rangeToList) range
      -- build the list of filenames to read.
      filenames <- rmap (fun (\i -> "./files/f"++show i++".txt")) indexes
      -- read each file 4 times (increase computation time)
      filenames' <- rflatmap (fun (\f -> L.replicate 4 f)) filenames
      -- creat source conduit
      sources <- rmap (fun sourceFile) filenames'
      -- read each each file and count the nb of occurence per characters
      countMaps <- rmap (funIO (\s -> runResourceT $ (s $$ CL.fold countChar M.empty))) sources

      -- reduce step
      zeroCountMap <- lconst M.empty
      reducedCount <- rfold' (foldFun reduceCharMap) (L.foldl' reduceCharMap M.empty) zeroCountMap countMaps

      r <- (\x -> ((), x)) <$$> reducedCount
      return r


jobDesc :: JobDesc () (M.Map Word8 Int)
jobDesc = MkJobDesc () (expGenerator 8) noReporting noIteration


slaveClosure :: Int -> Process ()
slaveClosure = CH.slaveProcess rpcConfigAction jobDesc


-- create remotables
remotable ['slaveClosure]


chClosure :: Int -> Closure (Process ())
chClosure = $(mkClosure 'slaveClosure)

rtable :: RemoteTable
rtable = __remoteTable initRemoteTable




-- main functions, choose the one you want to run.

mainCH1 :: IO ()
mainCH1 = runCloudHaskell rtable jobDesc1 chClosure1

mainLocal1 :: IO ()
mainLocal1 = do
  (_, r) <- runLocally True jobDesc1
  print r


-- create remotables
remotable ['slaveClosure]


chClosure :: Int -> Closure (Process ())
chClosure = $(mkClosure 'slaveClosure)


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


