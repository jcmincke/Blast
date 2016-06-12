{-# LANGUAGE ScopedTypeVariables #-}


module Main where

import Debug.Trace
import qualified  Data.List as L
import qualified  Data.Map as M
import            Control.Monad.IO.Class
import            Control.Monad.Logger
import            Control.Monad.Trans.State
import qualified  Data.Vault.Strict as V

--import Blast
--import qualified  Blast.Distributed.Rpc.Local as R

main :: IO ()
main = return ()

{-

er a = do
      r1 <- cstRdd [1..100000::Int]
      r2 <- smap r1 $ fun ((+) a)
      sum2 <- sfold r2 (+) 0
      r3 <- smap r1 (closure sum2 (\s a -> a+s))
      sum3 <- sfold r3 (+) 0
      r4 <- smap r2 (closure sum3 (\s a -> a+s))
      sum4 <- sfold r4 (+) 0
      a' <- cstLocal (a+1)
      r <- sfrom ((,) <$$> a' <**> sum4)
      return r

rrec = do
  (a,b) <- runStdoutLoggingT $ runRec False er 0 (\x -> x==10)
  print a
  print b

e:: StateT Int m (LocalExp (Int, Int))
e = do
      r1 <- cstRdd [1..1000::Int]
      r2 <- smap r1 $ fun ((+) 2)
      r3 <- smap r2 $ fun ((*) 2)
      r3bis <- smap r3 $ fun ((*) 2)
      a3 <- count r3bis
      r6 <- sfilter r3bis (closure a3 (\c a -> a>=c))
      --a3b <- count r6
    --  r6 = sfilter' r3 (fun (\a -> a>=10))
    --  r7 = count r6
    --  a4 = Collect () r5
      --a4 <- ((\a b -> (a,b)) <$$> a3) <**> a3b
       --in Collect () r6
    --  in collect r6
--      return a5
      --collect r3  --count r3
      a6 <- collect r6
      r7 <- sjoin r1 r1
      a7 <- collect r7
      a7bis <- collect r1

      a' <- collect r1
      r1f <- sflatmap r1 (closure a' (\_ x -> [x, x]))
--      r1f <- smap r1 (closure a3 (\_ x -> x))
      a1f <- collect r1f

      s6 <- sfold r6 (+) 0

      a8 <- sfrom ((,) <$$> s6  <**> a3)
--      a8 <- sfrom ((,) <$$> a3 <**> a7bis)
--      a8 <- sfrom ((,) <$$> a3 <**> a3)
      return a8
{-
e =  do
  r1 <- cstRdd [1..10::Int]
  r7 <- sjoin r1 r1
  a7 <- collect r7
  return a7
-}


rr = runStdoutLoggingT $ filterLogger (\_ _ -> False) $ do
  s <- R.createSimpleRemote 1.0 True 4 er
  liftIO $ putStrLn "start eval"
  r <- R.runSimpleLocalRec s True er 0 (\x -> False)
--  r <- R.runSimpleLocalRec s True er 0 (\x -> x==10)
  liftIO $ print r


--runSimpleLocalRec :: (S.Serialize a, S.Serialize b, RemoteClass s a) =>
-- s a -> Bool -> (a -> StateT Int IO (LocalExp (a, b))) -> a -> (a -> Bool) -> IO (a, b)

--createSimpleRemote :: (S.Serialize a) =>
--      Float -> Bool -> Int -> (a -> StateT Int IO (LocalExp (a, b)))
--      -> IO (SimpleRemote a)

{-
r = do
  e' <- evalStateT e 0
  print e'
  let r = runLocal e'
  print r
-}
-}

