{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns #-}


module Main where

--import Debug.Trace
import qualified  Data.List as L

import qualified  Data.Map.Strict as M

import            Control.Distributed.Process (RemoteTable, Process)
import            Control.Distributed.Process.Node (initRemoteTable)
import            Control.Distributed.Process.Closure (mkClosure, remotable)
import            Control.Distributed.Static (Closure)

import            Data.Conduit
import            Data.Conduit.List as CL
import            Data.Conduit.Binary as CB
import            Control.Monad.Trans.Resource
import            Blast
import            Blast.Syntax
import            Blast.Runner.CloudHaskell as CH

import qualified  Data.ByteString as BS
import            Data.Word

import            Common




countChar :: M.Map Word8 Int -> BS.ByteString -> M.Map Word8 Int
countChar m bs =
  BS.foldl' foldProc m bs
  where
  foldProc m' c = M.insertWith (+) c 1 m'


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

mainCH :: IO ()
mainCH = runCloudHaskell rtable jobDesc chClosure

mainLocal :: IO ()
mainLocal = do
  (_, r) <- runLocally True jobDesc
  print r


{-

  cd examples/WordCount

  Run Local:
    wordcount

  Run with CloudHaskell

    * start slaves:

        wordcount slave host port

    * start master:

        wordcount master host port

    ex:
      > wordcount slave localhost 5001
      > wordcount slave localhost 5002
      > wordcount master localhost 5000

-}

main :: IO ()
main = mainLocal

