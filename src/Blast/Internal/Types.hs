{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverlappingInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Blast.Internal.Types
where


import            Control.DeepSeq
import            Control.Lens (makeLenses)
import            Control.Monad.IO.Class
import            Control.Monad.Operational
import            Control.Monad.Trans.State
import            Data.Binary (Binary)
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import qualified  Data.Vector as Vc
import            GHC.Generics (Generic)



data Kind = Remote | Local

data Partition a =
  Singleton Int a
  |Many (Vc.Vector a)

instance Functor Partition where
  fmap f (Singleton size a) = Singleton size $ f a
  fmap f (Many as) = Many $ fmap f as

partitionToVector :: Partition a -> Vc.Vector a
partitionToVector (Singleton size a) = Vc.generate size (const a)
partitionToVector (Many as) = as

class Chunkable a where
  chunk :: Int -> a -> Partition a

class UnChunkable a where
  unChunk :: [a] -> a

class ChunkableFreeVar a where
  chunk' :: Int -> a -> Partition a
  chunk' n a = Singleton n a


instance ChunkableFreeVar a
instance ChunkableFreeVar ()




data Fun e a b =
  Pure (a -> IO b)
  |forall c . (S.Serialize c, ChunkableFreeVar c) => Closure (e 'Local c) (c -> a -> IO b)

data FoldFun e a r =
  FoldPure (r -> a -> IO r)
  |forall c . (S.Serialize c,ChunkableFreeVar c) => FoldClosure (e 'Local c) (c -> r -> a -> IO r)

data ExpClosure e a b =
  forall c . (S.Serialize c, ChunkableFreeVar c) => ExpClosure (e 'Local c) (c -> a -> IO b)

class Builder m e where
  makeRApply :: ExpClosure e a b -> e 'Remote a -> m (e 'Remote b)
  makeRConst :: (Chunkable a, S.Serialize a) => a -> m (e 'Remote a)
  makeLConst :: a -> m (e 'Local a)
  makeCollect :: (UnChunkable a, S.Serialize a) => e 'Remote a -> m (e 'Local a)
  makeLApply :: e 'Local (a -> b) -> e 'Local a -> m (e 'Local b)


data Syntax m e where
  StxRApply :: (Builder m e) => ExpClosure e a b -> e 'Remote a -> Syntax m (e 'Remote b)
  StxRConst :: (Builder m e, Chunkable a, S.Serialize a) => a -> Syntax m (e 'Remote a)
  StxLConst :: (Builder m e) => a -> Syntax m (e 'Local a)
  StxCollect :: (Builder m e, UnChunkable a, S.Serialize a) => e 'Remote a -> Syntax m (e 'Local a)
  StxLApply :: (Builder m e) => e 'Local (a -> b) -> e 'Local a -> Syntax m (e 'Local b)


rapply f a = singleton (StxRApply f a)
rconst a = singleton (StxRConst a)
lconst a = singleton (StxLConst a)
collect a = singleton (StxCollect a)
lapply f a = singleton (StxLApply f a)

build ::forall a m e. (Builder m e, MonadIO m) => ProgramT (Syntax m) m (e 'Local a) -> m (e 'Local a)
build p = do
    pv <- viewT p
    eval pv
    where
    eval :: (Builder m e, MonadIO m) => ProgramViewT (Syntax m) m (e 'Local a)  -> m (e 'Local a)
    eval (StxRApply f a :>>=  is) = do
      e <- makeRApply f a
      build (is e)
    eval (StxRConst a :>>=  is) = do
      e <- makeRConst a
      build (is e)
    eval (StxLConst a :>>=  is) = do
      e <- makeLConst a
      build (is e)
    eval (StxCollect a :>>=  is) = do
      e <- makeCollect a
      build (is e)
    eval (StxLApply f a :>>=  is) = do
      e <- makeLApply f a
      build (is e)

data Location = M | S



data JobDesc m a b = MkJobDesc {
  seed :: a
  , expGen :: forall e. (Builder m e) => a -> ProgramT (Syntax m) m (e 'Local (a, b)) --StateT Int m (LocalExp (a, b))
  , reportingAction :: a -> b -> IO a
  , recPredicate :: a -> Bool
  }




data Config = MkConfig
  {
    shouldOptimize :: Bool
    , slaveAvailability :: Float
  }


instance Chunkable [a] where
  chunk nbBuckets l =
    Many $ Vc.reverse $ Vc.fromList $ go [] nbBuckets l
    where
    go acc 1 ls = ls:acc
    go acc n ls = go (L.take nbPerBucket ls : acc) (n-1) (L.drop nbPerBucket ls)
    len = L.length l
    nbPerBucket = len `div` nbBuckets


instance UnChunkable [a] where
  unChunk l = L.concat l

