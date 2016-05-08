{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}

{-# LANGUAGE TemplateHaskell #-}

module Blast.Internal.Types
where

import Debug.Trace
import            Control.Bool (unlessM)
import            Control.Lens (makeLenses, set, view)
import            Control.Monad.IO.Class
import            Control.Monad.Trans.State
import qualified  Data.ByteString as BS
import            Data.IORef
import qualified  Data.List as L
import qualified  Data.Map as M
import            Data.Maybe
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import            GHC.Generics (Generic)

import            Unsafe.Coerce


data RemoteValue a =
  RemoteValue a
  |CachedRemoteValue

data ResultDescriptor b = ResultDescriptor Bool Bool  -- ^ should be returned + should be cached

instance Functor ResultDescriptor where
  fmap _ (ResultDescriptor sr sc) = (ResultDescriptor sr sc)

data CachedValType = CachedArg | CachedFreeVar
  deriving Show

data RemoteClosureResult b =
  RemCsResCacheMiss CachedValType
  |ExecRes (Maybe b)      -- Nothing when results are not returned
  |ExecResError String    --

type RemoteClosure = V.Vault -> (RemoteValue BS.ByteString) -> (RemoteValue BS.ByteString)
                     -> ResultDescriptor (BS.ByteString) -> (RemoteClosureResult BS.ByteString, V.Vault)

type Cacher = BS.ByteString -> V.Vault -> V.Vault
type UnCacher = V.Vault -> V.Vault
type IsCached = V.Vault -> Bool

data Info = Info {
  _nbRef :: Int
  , _remoteClosure :: Maybe RemoteClosure
  , _cacher :: Cacher
  , _unCache :: UnCacher
  , _getIsCached :: IsCached
  }

$(makeLenses ''Info)


type InfoMap = M.Map Int Info




data Rdd a = Rdd [a]
  deriving (Show, Generic, S.Serialize)

data Fun a b =
  Pure (a -> b)
  |forall c . (S.Serialize c, Show c) => Closure (LocalExp c) (c -> a -> b)

instance Functor (Fun a) where
  fmap g (Pure f) = Pure (g.f)
  fmap g (Closure c f) = Closure c (\c -> fmap g (f c))



data RemoteExp a where
  Trans :: (S.Serialize a, S.Serialize b) => Int -> V.Key (Rdd b) -> RemoteExp (Rdd a) -> (Fun a (Maybe b)) -> RemoteExp (Rdd b)
  ConstRemote :: (S.Serialize a) => Int -> V.Key a -> a -> RemoteExp a
  Join :: (S.Serialize a, S.Serialize b) => Int -> V.Key (Rdd (a,b)) -> RemoteExp (Rdd a) -> RemoteExp (Rdd b) -> RemoteExp (Rdd (a,b))

data LocalExp a where
  Fold :: (S.Serialize a, S.Serialize b) => Int -> V.Key b -> RemoteExp (Rdd a) -> (b -> a -> b) -> b -> LocalExp b
  ConstLocal :: (S.Serialize a) => Int -> V.Key a -> a -> LocalExp a
  Collect :: (S.Serialize a) => Int -> V.Key (Rdd a) -> RemoteExp (Rdd a) -> LocalExp (Rdd a)
--  Apply :: (S.Serialize b) => Int -> V.Key b -> LocalExp (a->b) -> LocalExp a -> LocalExp b
  FMap :: (S.Serialize a, S.Serialize b) => Int -> V.Key b -> (a->b) -> LocalExp a -> LocalExp b
  FromAppl :: (S.Serialize a) => Int -> V.Key a -> ApplExp a -> LocalExp a

data ApplExp a where
  Apply' :: (S.Serialize a) => ApplExp (a->b) -> LocalExp a -> ApplExp b
  ConstAppl :: a -> ApplExp a






instance Show (RemoteExp a) where
  show (Trans i _ e (Pure _)) = show ("Trans pure"::String, i,  e)
  show (Trans i _ e (Closure ce _)) = show ("Trans closure"::String, i,  e, ce)
  show (ConstRemote i _ r) = show ("ConstRemote"::String, i)
  show (Join i _ a b) = show ("Join"::String, i, a, b)

instance Show (LocalExp a) where
  show (Fold i _ e _ _) = show ("Fold"::String, i, e)
  show (ConstLocal i _ r) = show ("ConstLocal"::String, i)
  show (Collect i _ a) = show ("Collect"::String, i, show a)
--  show (Apply i _ _ a) = show ("Apply"::String, i, show a)
  show (FromAppl i _ a) = show ("FromAppl"::String, i, show a)
  show (FMap i _ _ a) = show ("FMap"::String, i, show a)

instance Show (ApplExp a) where
  show (Apply' f e) = show ("Apply'"::String, f, e)
  show (ConstAppl a) = show ("ConstAppl"::String)


-- TODO to improve
partitionRdd :: Int -> Rdd a -> [Rdd a]
partitionRdd nbBuckets (Rdd l) =
  L.map Rdd $ L.reverse $ go [] nbBuckets l
  where
  go acc 1 l = l:acc
  go acc n l = go (L.take nbPerBucket l : acc) (n-1) (L.drop nbPerBucket l)
  len = L.length l
  nbPerBucket = len `div` nbBuckets




