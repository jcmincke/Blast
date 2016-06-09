{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
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
import            Control.Monad.Trans.State
import            Data.Binary (Binary)
import qualified  Data.ByteString as BS
import qualified  Data.List as L
import qualified  Data.Map as M
import qualified  Data.Serialize as S
import qualified  Data.Vault.Strict as V
import            Data.Vector as Vc
import            GHC.Generics (Generic)


data CachedValType = CachedArg | CachedFreeVar
  deriving (Show, Generic)

data RemoteClosureResult =
  RemCsResCacheMiss CachedValType
  |ExecRes
  |ExecResError String    --
  deriving (Generic, Show)


instance NFData RemoteClosureResult
instance NFData CachedValType

type RemoteClosureImpl = V.Vault -> IO (RemoteClosureResult, V.Vault)

type Cacher = BS.ByteString -> V.Vault -> V.Vault
type CacherReader = V.Vault -> Maybe BS.ByteString
type UnCacher = V.Vault -> V.Vault
type IsCached = V.Vault -> Bool


data Info = Info {
  _nbRef :: Int
  , _info :: NodeTypeInfo
  }

data NodeTypeInfo =
  NtRMap RMapInfo
  |NtRConst RConstInfo
  |NtLExp LExpInfo
  |NtLExpNoCache

data RMapInfo = MkRMapInfo {
  _rmRemoteClosure :: RemoteClosureImpl
  , _rmUnCacher :: UnCacher
  , _rmCacheReader :: Maybe CacherReader
  }

data RConstInfo = MkRConstInfo {
  _rcstCacher :: Cacher
  , _rcstUnCacher :: UnCacher
  , _rcstCacheReader :: Maybe CacherReader
  }

data LExpInfo = MkLExpInfo {
  _lexpCacher :: Cacher
  , _lexpUnCacher :: UnCacher
  }

$(makeLenses ''Info)

type InfoMap = M.Map Int Info

data Fun k a b =
  Pure (a -> IO b)
  |forall c . (S.Serialize c, Show c, ChunkableFreeVar c) => Closure (LocalExp k c) (c -> a -> IO b)

data FoldFun k a r =
  FoldPure (r -> a -> IO r)
  |forall c . (S.Serialize c, Show c, ChunkableFreeVar c) => FoldClosure (LocalExp k c) (c -> r -> a -> IO r)


data ExpClosure k a b =
  forall c . (S.Serialize c, Show c, ChunkableFreeVar c) => ExpClosure (LocalExp k c) (c -> a -> IO b)

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


data Kind = Master | Slave

data RemoteExpCtor = RMapCtor | RConstCtor
data LocalExpCtor = LConstCtor | CollectCtor | FMapCtor


newtype Ann a = Ann a

type family RMapAnn (k::Kind) a where
  RMapAnn 'Master a = Ann ()
  RMapAnn 'Slave a = Ann (V.Key a)

type family RConstAnn (k::Kind) a where
  RConstAnn 'Master a = Ann ()
  RConstAnn 'Slave a = Ann (V.Key a)


type family LConstAnn (k::Kind) a where
  LConstAnn 'Master a = Ann (LocalKey a)
  LConstAnn 'Slave a = Ann (V.Key a)

type family FMapAnn (k::Kind) a where
  FMapAnn 'Master a = Ann (LocalKey a)
  FMapAnn 'Slave a = Ann (V.Key a)

type family CollectAnn (k::Kind) a where
  CollectAnn 'Master a = Ann (LocalKey a)
  CollectAnn 'Slave a = Ann (V.Key a)


data RemoteExp (k::Kind) a where
  RMap :: Int -> RMapAnn k b -> ExpClosure k a b -> RemoteExp k a -> RemoteExp k b
  RConst :: (S.Serialize a, Chunkable a) => Int -> RConstAnn k a -> a -> RemoteExp k a

type LocalKey a = V.Key (a, Maybe (Partition BS.ByteString))

data LocalExp (k::Kind) a where
  LConst :: Int -> LConstAnn k a -> a -> LocalExp k a
  Collect :: (S.Serialize a, UnChunkable a) => Int -> CollectAnn k a -> RemoteExp k a -> LocalExp k a
  FMap :: Int -> FMapAnn k b -> LocalExp k (a -> b) -> LocalExp k a -> LocalExp k b


instance Show (RemoteExp (k::Kind) a) where
  show (RMap i ann (ExpClosure ce _) e) = show ("Map closure"::String, i, e)
  show (RConst i ann _) = show ("ConstRemote"::String, i)

--instance Show (RemoteExp 'Slave a) where
--  show (RMap ann (ExpClosure ce _) e) = show ("Map closure"::String, ann,  e, ce)
--  show (RConst ann _) = show ("ConstRemote"::String, ann)

instance Show (LocalExp (k::Kind) a) where
  show (LConst i ann _) = show ("ConstLocal"::String, i)
  show (Collect i ann a) = show ("Collect"::String, i, show a)
  show (FMap i ann _ e) = show ("FMap"::String, i, e)

--showLConst :: 'LConst

{-}
instance Show (LocalExp 'Slave a) where
  show (LConst ann _) = show ("ConstLocal"::String, ann)
  show (Collect ann a) = show ("Collect"::String, ann, show a)
  show (FMap ann _ e) = show ("FMap"::String, ann,  e)

instance (Show (LAnnotation k 'LConstCtor a)
         ,Show (LAnnotation k 'CollectCtor a)
         ,Show (LAnnotation k 'FMapCtor a)) =>
         Show (LocalExp k a) where
  show (LConst ann _) = show ("ConstLocal"::String, ann)
  show (Collect ann a) = show ("Collect"::String, ann, show a)
  show (FMap ann _ e) = show ("FMap"::String, ann,  e)
-}

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
  , expGen :: forall k. a -> StateT Int m (LocalExp k (a, b))
  , reportingAction :: a -> b -> IO a
  , recPredicate :: a -> Bool
  }


instance Binary RemoteClosureResult
instance Binary CachedValType

data Config = MkConfig
  {
    shouldOptimize :: Bool
    , slaveAvailability :: Float
  }


