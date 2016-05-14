{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module Blast.Internal.Types
where

import            Control.DeepSeq
import            Control.Lens (makeLenses)
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import            GHC.Generics (Generic)



data RemoteValue a =
  RemoteValue a
  |CachedRemoteValue
  deriving Generic

data ResultDescriptor b = ResultDescriptor Bool Bool  -- ^ should be returned + should be cached
  deriving Generic

instance Functor ResultDescriptor where
  fmap _ (ResultDescriptor sr sc) = (ResultDescriptor sr sc)

data CachedValType = CachedArg | CachedFreeVar
  deriving (Show, Generic)

data RemoteClosureResult b =
  RemCsResCacheMiss CachedValType
  |ExecRes (Maybe b)      -- Nothing when results are not returned
  |ExecResError String    --
  deriving Generic


instance NFData (RemoteClosureResult BS.ByteString)
instance NFData CachedValType
instance NFData (ResultDescriptor BS.ByteString)
instance NFData (RemoteValue BS.ByteString)

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
  fmap g (Closure ce f) = Closure ce (\c -> fmap g (f c))


data RemoteExp a where
  Map :: (S.Serialize a, S.Serialize b) => Int -> V.Key (Rdd b) -> RemoteExp (Rdd a) -> (Fun a (Maybe b)) -> RemoteExp (Rdd b)
  FlatMap :: (S.Serialize a, S.Serialize b) => Int -> V.Key (Rdd b) -> RemoteExp (Rdd a) -> (Fun a [b]) -> RemoteExp (Rdd b)
  ConstRemote :: (S.Serialize a) => Int -> V.Key a -> a -> RemoteExp a

data LocalExp a where
  Fold :: (S.Serialize a, S.Serialize b) => Int -> V.Key b -> RemoteExp (Rdd a) -> (b -> a -> b) -> b -> LocalExp b
  ConstLocal :: (S.Serialize a) => Int -> V.Key a -> a -> LocalExp a
  Collect :: (S.Serialize a) => Int -> V.Key (Rdd a) -> RemoteExp (Rdd a) -> LocalExp (Rdd a)
  FMap :: (S.Serialize a, S.Serialize b) => Int -> V.Key b -> (a->b) -> LocalExp a -> LocalExp b
  FromAppl :: (S.Serialize a) => Int -> V.Key a -> ApplExp a -> LocalExp a

data ApplExp a where
  Apply' :: (S.Serialize a) => ApplExp (a->b) -> LocalExp a -> ApplExp b
  ConstAppl :: a -> ApplExp a


instance Show (RemoteExp a) where
  show (Map i _ e (Pure _)) = show ("Map pure"::String, i,  e)
  show (Map i _ e (Closure ce _)) = show ("Map closure"::String, i,  e, ce)
  show (FlatMap i _ e (Pure _)) = show ("FlatMap pure"::String, i,  e)
  show (FlatMap i _ e (Closure ce _)) = show ("FlatMap closure"::String, i,  e, ce)
  show (ConstRemote i _ _) = show ("ConstRemote"::String, i)

instance Show (LocalExp a) where
  show (Fold i _ e _ _) = show ("Fold"::String, i, e)
  show (ConstLocal i _ _) = show ("ConstLocal"::String, i)
  show (Collect i _ a) = show ("Collect"::String, i, show a)
  show (FromAppl i _ a) = show ("FromAppl"::String, i, show a)
  show (FMap i _ _ a) = show ("FMap"::String, i, show a)

instance Show (ApplExp a) where
  show (Apply' f e) = show ("Apply'"::String, f, e)
  show (ConstAppl _) = show ("ConstAppl"::String)


-- TODO to improve
partitionRdd :: Int -> Rdd a -> [Rdd a]
partitionRdd nbBuckets (Rdd l) =
  L.map Rdd $ L.reverse $ go [] nbBuckets l
  where
  go acc 1 ls = ls:acc
  go acc n ls = go (L.take nbPerBucket ls : acc) (n-1) (L.drop nbPerBucket ls)
  len = L.length l
  nbPerBucket = len `div` nbBuckets




