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



sa :: MonadResource m => Conduit i m BS.ByteString --ConduitM i ByteString m ()
sa = sourceFile "./files/wordcount/f1.txt"

sb :: MonadResource m => Conduit BS.ByteString m o -- ConduitM ByteString o m ()
sb = sinkFile "./files/wordcount/f1-2.txt"

sc:: MonadResource m => Conduit i m BS.ByteString -> Conduit i m BS.ByteString
sc  a = mapOutput (\bs ->
              let l = L.reverse $ BS.unpack bs
              in  BS.pack l) a

sd::MonadResource m => Conduit i m BS.ByteString
sd = sc sa

countChar :: M.Map Word8 Int -> BS.ByteString -> M.Map Word8 Int
countChar m bs =
  BS.foldl' foldProc m bs
  where
  foldProc m' c = M.insertWith (+) c 1 m'

createSource :: MonadResource m => FilePath -> ConduitM i BS.ByteString m ()
createSource f = sourceFile f

reduceCharMap :: M.Map Word8 Int -> M.Map Word8 Int -> M.Map Word8 Int
reduceCharMap acc m = M.unionWith (+) acc m

expGenerator :: Int -> () -> LocalComputation ((), M.Map Word8 Int)
expGenerator nbFiles () = do
      range <- rconst $ Range 1 (nbFiles+1)
      indexes <- rapply (fun rangeToList) range
      filenames <- rmap (fun (\i -> "./files/wordcount/f"++show i++".txt")) indexes
      filenames' <- rflatmap (fun (\f -> L.replicate 4 f)) filenames
      sources <- rmap (fun sourceFile) filenames'
      countMaps <- rmap (funIO (\s -> runResourceT $ (s $$ CL.fold countChar M.empty))) sources
      zeroCountMap <- lconst M.empty
      reducedCount <- rfold' (foldFun reduceCharMap) (L.foldl' reduceCharMap M.empty) zeroCountMap countMaps
      let lres = reducedCount
      r <- (\x -> ((), x)) <$$> lres
      return r


jobDesc :: JobDesc () (M.Map Word8 Int)
jobDesc = MkJobDesc () (expGenerator 8) reporting (\_ _ _ -> True)


rloc:: Bool -> IO ()
rloc optimize = do
  let cf = defaultConfig { shouldOptimize = optimize }
  (a,b) <- logger $ Loc.runRec 4 cf jobDesc
  print a
  print b
  return ()
  where
  logger a = runLoggingT a (\_ _ _ _ -> return ())



reporting :: forall t b. b -> t -> IO b
reporting a _ = do
  putStrLn "Reporting"
  putStrLn "End Reporting"
  return a


rpcConfigAction :: IO RpcConfig
rpcConfigAction = return $
  MkRpcConfig
    (defaultConfig { shouldOptimize = False })
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


