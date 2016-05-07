module Main where

import qualified  Data.Map as M
import            Control.Monad.Trans.State
import qualified  Data.Vault.Strict as V

import Blast

main :: IO ()
main = return ()


e = exp
  where
--  exp :: StateT Int m (LocalExp (Int, Int))
  exp = do
      r1 <- cstRdd [1..10::Int]
      r2 <- smap r1 $ fun ((+) 2)
      r3 <- smap r2 $ fun ((*) 2)
      r3bis <- smap r3 $ fun ((*) 2)
      a3 <- count r3bis
      --ba3 <- (,) <$$> a3 <**> a3
    --  r4 = cstRdd [1..10::Int]
    --  r5 = join r2 r4
  --    r6 <- sfilter r3 (fun (\a -> a>=10))
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
      a7 <- (,) <$$> a3 <**> a6
      return a7

r = do
  e' <- evalStateT e 0
  print e'
  let r = runLocal e'
  print r

rd1 = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  r <- evalStateT (runLocalD infos e') V.empty
  print r


rd2 = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  (infos', e'') <- optimize count infos e'
  print e''
  r <- evalStateT (runLocalD infos' e'') V.empty
  print r
  where