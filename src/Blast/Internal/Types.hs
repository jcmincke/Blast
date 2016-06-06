{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverlappingInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}


module Blast.Internal.Types
where


import            Control.DeepSeq
import            Control.Lens (makeLenses)
import            Control.Monad.Trans.State
import            Data.Binary (Binary)
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import            Data.Vector as Vc
import            GHC.Generics (Generic)

{-
data RemoteValue a =
  RemoteValue a
  |CachedRemoteValue
  deriving (Generic, Show)
-}

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

type RemoteClosureImpl = V.Vault -> ResultDescriptor (BS.ByteString) -> IO (RemoteClosureResult BS.ByteString, V.Vault)

type Cacher = BS.ByteString -> V.Vault -> V.Vault
type UnCacher = V.Vault -> V.Vault
type IsCached = V.Vault -> Bool

data CacheInfo = MkCacheInfo {
  _cacher :: Cacher
  , _unCache :: UnCacher
  , _getIsCached :: IsCached
  }

data Info = Info {
  _nbRef :: Int
  , _remoteClosure :: Maybe RemoteClosureImpl
  , _cacheInfo :: Maybe CacheInfo
  }

$(makeLenses ''Info)

type InfoMap = M.Map Int Info

data Rdd a = Rdd [a]
  deriving (Show, Generic, S.Serialize)

data Fun a b =
  Pure (a -> IO b)
  |forall c . (S.Serialize c, Show c, ChunkableFreeVar c) => Closure (LocalExp c) (c -> a -> IO b)

data FoldFun a r =
  FoldPure (r -> a -> IO r)
  |forall c . (S.Serialize c, Show c, ChunkableFreeVar c) => FoldClosure (LocalExp c) (c -> r -> a -> IO r)

data ExpClosure a b =
  forall c . (S.Serialize c, Show c, ChunkableFreeVar c) => ExpClosure (LocalExp c) (c -> a -> IO b)

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

data RemoteExp a where
  RMap :: Int -> V.Key b -> ExpClosure a b -> RemoteExp a -> RemoteExp b
  RConst :: (S.Serialize a, Chunkable a) => Int -> V.Key a -> a -> RemoteExp a

type LocalKey a = V.Key (a, Maybe (Partition BS.ByteString))

data LocalExp a where
  LConst :: Int -> LocalKey a -> a -> LocalExp a
  Collect :: (S.Serialize a, UnChunkable a) => Int -> LocalKey a -> RemoteExp a -> LocalExp a
  FMap :: Int -> LocalKey b -> LocalExp (a -> b) -> LocalExp a -> LocalExp b


instance Show (RemoteExp a) where
  show (RMap i _ (ExpClosure ce _) e) = show ("Map closure"::String, i,  e, ce)
  show (RConst i _ _) = show ("ConstRemote"::String, i)

instance Show (LocalExp a) where
  show (LConst i _ _) = show ("ConstLocal"::String, i)
  show (Collect i _ a) = show ("Collect"::String, i, show a)
  show (FMap i _ _ e) = show ("FMap"::String, i,  e)


-- TODO to improve
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


class NodeIndexer s where
  nextIndex :: (Monad m) => StateT s m Int

instance NodeIndexer Int where
  nextIndex = do
    index <- get
    put (index+1)
    return index

instance NodeIndexer (Int, a) where
  nextIndex = do
    (index, a) <- get
    put (index+1, a)
    return index


data JobDesc m a b = MkJobDesc {
  seed :: a
  , expGen :: a -> StateT Int m (LocalExp (a, b))
  , reportingAction :: a -> b -> IO a
  , recPredicate :: a -> Bool
  }


instance Binary (RemoteClosureResult BS.ByteString)
instance Binary CachedValType
instance Binary (ResultDescriptor BS.ByteString)
--instance Binary (RemoteValue BS.ByteString)

data Config = MkConfig
  {
    shouldOptimize :: Bool
    , slaveAvailability :: Float
  }


