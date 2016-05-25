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
  deriving (Generic, Show)

data ResultDescriptor b = ResultDescriptor Bool Bool  -- ^ should be returned + should be cached
  deriving (Generic, Show)

instance Functor ResultDescriptor where
  fmap _ (ResultDescriptor sr sc) = (ResultDescriptor sr sc)

data CachedValType = CachedArg | CachedFreeVar
  deriving (Show, Generic)

data RemoteClosureResult b =
  RemCsResCacheMiss CachedValType
  |ExecRes (Maybe b)      -- Nothing when results are not returned
  |ExecResError String    --
  deriving (Generic, Show)


instance NFData (RemoteClosureResult BS.ByteString)
instance NFData CachedValType
instance NFData (ResultDescriptor BS.ByteString)
instance NFData (RemoteValue BS.ByteString)

type RemoteClosureImpl = V.Vault -> (RemoteValue BS.ByteString) -> (RemoteValue BS.ByteString)
                     -> ResultDescriptor (BS.ByteString) -> (RemoteClosureResult BS.ByteString, V.Vault)

type Cacher = BS.ByteString -> V.Vault -> V.Vault
type UnCacher = V.Vault -> V.Vault
type IsCached = V.Vault -> Bool

data Info = Info {
  _nbRef :: Int
  , _remoteClosure :: Maybe RemoteClosureImpl
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

data FoldFun a r =
  FoldPure (r -> a -> r)
  |forall c . (S.Serialize c, Show c) => FoldClosure (LocalExp c) (c -> r -> a -> r)

data ExpClosure a b =
  forall c . (S.Serialize c, Show c) => ExpClosure (LocalExp c) (c -> a -> b)

data PreparedFoldClosure a r =
  forall c . (S.Serialize c, Show c, S.Serialize r) => PreparedFoldClosure (LocalExp (c, r)) (c -> r -> a -> r)

class Chunkable a where
  chunk :: Int -> a -> [a]
  unChunk :: [a] -> a

data RemoteExp a where
  RMap :: (S.Serialize a, S.Serialize b, Chunkable a, Chunkable b) => Int -> V.Key b -> RemoteExp a -> ExpClosure a b -> RemoteExp b
--  RFold :: (S.Serialize a, S.Serialize r) => Int -> V.Key (Rdd r) -> RemoteExp (Rdd a) -> PreparedFoldClosure a r -> RemoteExp (Rdd r)
--  RFlatMap :: (S.Serialize a, S.Serialize b) => Int -> V.Key (Rdd b) -> RemoteExp (Rdd a) -> (Fun a [b]) -> RemoteExp (Rdd b)
  RConst :: (S.Serialize a, Chunkable a) => Int -> V.Key a -> a -> RemoteExp a

data LocalExp a where
--  LFold :: (S.Serialize a, S.Serialize b) => Int -> V.Key b -> RemoteExp (Rdd a) -> PreparedFoldClosure a b -> LocalExp b
  LConst :: (S.Serialize a) => Int -> V.Key a -> a -> LocalExp a
  Collect :: (S.Serialize a, Chunkable a) => Int -> V.Key a -> RemoteExp a -> LocalExp a
  LMap :: (S.Serialize a, S.Serialize b) => Int -> V.Key b -> LocalExp a -> ExpClosure a b -> LocalExp b
  FromAppl :: (S.Serialize a) => Int -> V.Key a -> ApplyExp a -> LocalExp a

data ApplyExp a where
  Apply :: ApplyExp (a->b) -> LocalExp a -> ApplyExp b
  ConstApply :: a -> ApplyExp a

instance Show (RemoteExp a) where
--  show (RMap i _ e (Pure _)) = show ("Map pure"::String, i,  e)
  show (RMap i _ e (ExpClosure ce _)) = show ("Map closure"::String, i,  e, ce)
--  show (RFold i _ e (PreparedFoldClosure ce _)) = show ("RemoteFold closure"::String, i,  e, ce)
--  show (RFlatMap i _ e (Pure _)) = show ("FlatMap pure"::String, i,  e)
--  show (RFlatMap i _ e (Closure ce _)) = show ("FlatMap closure"::String, i,  e, ce)
  show (RConst i _ _) = show ("ConstRemote"::String, i)

instance Show (LocalExp a) where
--  show (LFold i _ e _) = show ("Fold"::String, i, e)
  show (LConst i _ _) = show ("ConstLocal"::String, i)
  show (Collect i _ a) = show ("Collect"::String, i, show a)
  show (FromAppl i _ a) = show ("FromAppl"::String, i, show a)
  show (LMap i _ a _) = show ("FMap"::String, i, show a)

instance Show (ApplyExp a) where
  show (Apply f e) = show ("Apply'"::String, f, e)
  show (ConstApply _) = show ("ConstApply"::String)


-- TODO to improve
instance Chunkable [a] where
  chunk nbBuckets l =
    L.reverse $ go [] nbBuckets l
    where
    go acc 1 ls = ls:acc
    go acc n ls = go (L.take nbPerBucket ls : acc) (n-1) (L.drop nbPerBucket ls)
    len = L.length l
    nbPerBucket = len `div` nbBuckets
  unChunk l = L.concat l

{-}
partitionRdd :: Int -> Rdd a -> [Rdd a]
partitionRdd nbBuckets (Rdd l) =
  L.map Rdd $ L.reverse $ go [] nbBuckets l
  where
  go acc 1 ls = ls:acc
  go acc n ls = go (L.take nbPerBucket ls : acc) (n-1) (L.drop nbPerBucket ls)
  len = L.length l
  nbPerBucket = len `div` nbBuckets
-}



