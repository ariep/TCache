{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
module Data.TCache.ID
  (
    ID(ID)
  , WithID(WithID,_object,__ID)
  , object,_ID
  , new
  , Ref
  , newRef
  , ref
  , deref
  , fromID
  , fromIDMaybe
  , derefObject
  , refLens
  , listWithID
  , delete
  , update
  ) where

import qualified Data.TCache       as T
import qualified Data.TCache.Defs  as T
import qualified Data.TCache.Index as T
import qualified Data.TCache.Index.Map as IndexMap

import           Control.Applicative   ((<$>))
import           Control.Lens.Monadic  (MLens,mkMLens)
import           Data.Functor          (void)
import           Data.Functor.Identity (Identity(Identity))
import qualified Data.Serialize   as C
import           Data.Serialize.Text   ()
import qualified Data.Map         as Map
import qualified Data.Set         as Set
import           Data.Text             (Text,pack,unpack)
import           Data.Traversable      (for)
import           Data.Typeable         (Typeable,typeRep,Proxy(Proxy))
import           GHC.Generics          (Generic)
import           Prelude hiding (lookup)
import           System.Random         (newStdGen,randomRs)


newtype ID a
  = ID Text
  deriving (Eq,Ord,Generic,Typeable,Show,Read)
instance C.Serialize (ID a) where

data WithID a
  = WithID
    {
      __ID :: ID a
    , _object :: a
    }
  deriving (Eq,Generic,Typeable,Show)

deriving instance (Read a) => Read (WithID a)

object :: Functor f => (a -> f b) -> WithID a -> f (WithID b)
object f (WithID (ID t) a) = fmap (WithID (ID t)) (f a)

_ID :: Functor f => (ID a -> f (ID a)) -> WithID a -> f (WithID a)
_ID f (WithID i a) = fmap (flip WithID a) (f i)

instance T.Indexable (WithID a) where
  key (WithID (ID t) _) = unpack t

new :: forall a.
  ( T.IResource (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set T.Key))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) Identity (ID a)))
  ) => T.Persist -> a -> T.STM (WithID a)
new store x = loop where
  loop = do
    i <- ID <$> T.unsafeIOToSTM (randomText 8)
    existing <- IndexMap.lookup store (IndexMap.field (__ID :: WithID a -> ID a)) i
    if Set.null existing
      then return $ WithID i x
      else loop

randomText :: Int -> IO Text
randomText l = pack . take l . randomRs ('a','z') <$> newStdGen

type Ref a
  = T.DBRef (WithID a)

newRef ::
  ( T.IResource (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set T.Key))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) Identity (ID a)))
  ) => T.Persist -> a -> T.STM (ID a)
newRef store x = do
  withID <- new store x
  T.newDBRef store withID
  return $ __ID withID

delete :: (T.IResource (WithID a),Typeable a)
  => T.Persist -> Ref a -> T.STM ()
delete store = T.delDBRef store

update :: (T.IResource (WithID a),Typeable a)
  => T.Persist -> Ref a -> a -> T.STM (Maybe ())
update store r x = T.readDBRef store r >>= \case
  Just (WithID i _) -> Just <$> T.writeDBRef store r (WithID i x)
  Nothing           -> return Nothing

ref :: forall a. (T.IResource (WithID a),Typeable a)
  => T.Persist -> ID a -> Ref a
ref store i = T.getDBRef store $ T.key (WithID i undefined :: WithID a)

deref :: (T.IResource (WithID a),Typeable a)
  => T.Persist -> Ref a -> T.STM (WithID a)
deref store r = maybe err id <$> T.readDBRef store r where
  err = error $ "Data.TCache.ID.deref: ID " ++ show r ++ " does not occur in database"

fromID :: (T.IResource (WithID a),Typeable a)
  => T.Persist -> ID a -> T.STM (WithID a)
fromID store i = deref store $ ref store i

fromIDMaybe :: (T.IResource (WithID a),Typeable a)
  => T.Persist -> ID a -> T.STM (Maybe (WithID a))
fromIDMaybe store i = T.readDBRef store $ ref store i

derefObject :: (T.IResource (WithID a),Typeable a)
  => T.Persist -> Ref a -> T.STM a
derefObject store r = _object <$> deref store r

refLens :: (T.IResource (WithID a),Typeable a) =>
  T.Persist -> MLens T.STM (Ref a) (Maybe ()) a a
refLens store = mkMLens (fmap _object . deref store) (update store)

listWithID :: forall a.
  ( T.IResource (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set T.Key))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) Identity (ID a)))
  ) => T.Persist -> T.STM [WithID a]
listWithID store = do
  ids <- IndexMap.listAll store $ IndexMap.field (__ID :: WithID a -> ID a)
  (concat <$>) . for ids $ \ (_i,refs) -> case Set.toList refs of
    [r] -> (: []) <$> deref store (T.getDBRef store r)
    []  -> return []
