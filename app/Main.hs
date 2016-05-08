module Main where

import qualified  Data.List as L
import qualified  Data.Map as M
import            Control.Monad.Trans.State
import qualified  Data.Vault.Strict as V

import Blast
import qualified  Blast.Remote as R

main :: IO ()
main = rr


e = exp
  where
--  exp :: StateT Int m (LocalExp (Int, Int))
  exp = do
      r1 <- cstRdd [1..4::Int]
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
      a8 <- sfrom ((,) <$$> a3 <**> a7)
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

rr = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  (infos', e'') <- optimize count infos e'
  print e''
  s <- R.createSimpleRemote e'' 2
  putStrLn "start eval"
  r <- evalStateT (R.runSimpleLocal infos' e'') (s, V.empty)
  print r

r = do
  e' <- evalStateT e 0
  print e'
  let r = runLocal e'
  print r


rd1 = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  putStrLn "start eval"
  print $ M.keys infos
  let (Info _ csm _ _ _) =  infos M.! 1
  case csm of
    Just _ -> putStrLn "c'est just"
    Nothing -> putStrLn "c'est Nothing"

  r <- evalStateT (runLocalD infos e') V.empty
  print r


rdcaching1 = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  putStrLn "start eval"

  r <- evalStateT (runLocalCachingD infos e') V.empty
  print r


rd2 = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  (infos', e'') <- optimize count infos e'
  print e''
  r <- evalStateT (runLocalD infos' e'') V.empty
  print r




rdcaching2 = do
  (e', count) <- runStateT e 0
  print e'
  infos <- execStateT (analyseLocal e') M.empty
  (infos', e'') <- optimize count infos e'
  print e''
  r <- evalStateT (runLocalCachingD infos' e'') V.empty
  print r







rff = Rdd [1..100]












