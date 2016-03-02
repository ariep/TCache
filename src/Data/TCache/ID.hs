{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
module Data.TCache.ID
  ( ID(ID)
  , WithID(WithID, _object, __ID)
  , object, _ID
  , fromKey
  , new
  , addNew
  , Ref
  , ref
  , refM
  , deref
  , lookup
  , lookupMaybe
  , derefObject
  , refLens
  , _IDLens
  , listWithID
  , delete
  , update
  , write
  ) where

import qualified Data.TCache       as T
import qualified Data.TCache.Defs  as T
import qualified Data.TCache.Index as T
import qualified Data.TCache.Index.Map as IndexMap

import           Control.Lens.Monadic  (MLens, mkMLens)
import           Data.Functor          (void)
import           Data.Functor.Identity (Identity(Identity))
import           Data.ID (ID(..), WithID(..), object, _ID, randomID)
import qualified Data.Serialize   as C
import           Data.Serialize.Text   ()
import qualified Data.Map         as Map
import qualified Data.Set         as Set
import           Data.Text             (pack, unpack)
import           Data.Traversable      (for)
import           Data.Typeable         (Typeable, typeRep, Proxy(Proxy))
import           GHC.Generics          (Generic)
import           Prelude hiding (lookup)


instance T.Indexable (WithID a) where
  key (WithID (ID t) _) = unpack t

fromKey :: T.Key -> ID a
fromKey = ID . pack

new :: forall a.
  ( T.IResource (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set T.Key))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) (ID a)))
  ) => a -> T.DB (WithID a)
new x = loop where
  loop = do
    i <- T.stm $ T.unsafeIOToSTM (randomID 8)
    exists <- IndexMap.lookup (IndexMap.field (__ID :: WithID a -> ID a)) i
    if Set.null exists
      then return $ WithID i x
      else loop

type Ref a
  = T.DBRef (WithID a)

addNew ::
  ( T.IResource (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set T.Key))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) (ID a)))
  ) => a -> T.DB (ID a)
addNew x = do
  withID <- new x
  T.newDBRef withID
  return $ __ID withID

delete :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.DB ()
delete = T.delDBRef

update :: (T.IResource (WithID a),Typeable a)
  => Ref a -> a -> T.DB (Maybe ())
update r x = T.readDBRef r >>= \case
  Just (WithID i _) -> Just <$> T.writeDBRef r (WithID i x)
  Nothing           -> return Nothing

write :: (T.IResource (WithID a),Typeable a)
  => WithID a -> T.DB (Maybe ())
write w = flip update (_object w) =<< refM (__ID w)

ref :: forall a. (T.IResource (WithID a),Typeable a)
  => T.Persist -> ID a -> Ref a
ref store i = T.getDBRef store $ T.key (WithID i undefined :: WithID a)

refM :: forall a. (T.IResource (WithID a), Typeable a)
  => ID a -> T.DB (Ref a)
refM i = T.db $ \ s -> return $ ref s i

deref :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.DB (WithID a)
deref r = maybe err id <$> T.readDBRef r where
  err = error $ "Data.TCache.ID.deref: ID " ++ show r ++ " does not occur in database"

lookup :: (T.IResource (WithID a),Typeable a)
  => ID a -> T.DB (WithID a)
lookup i = deref =<< refM i

lookupMaybe :: (T.IResource (WithID a),Typeable a)
  => ID a -> T.DB (Maybe (WithID a))
lookupMaybe i = T.readDBRef =<< refM i

derefObject :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.DB a
derefObject r = _object <$> deref r

refLens :: (T.IResource (WithID a),Typeable a) =>
  MLens T.DB (Ref a) (Maybe ()) a a
refLens = mkMLens (fmap _object . deref) update

_IDLens :: (T.IResource (WithID a),Typeable a) =>
  MLens T.DB (ID a) (Maybe ()) a a
_IDLens = mkMLens (fmap _object . lookup) (\ i a -> write (WithID i a))

listWithID :: forall a.
  ( T.IResource (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set T.Key))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) (ID a)))
  ) => T.DB [WithID a]
listWithID = do
  ids <- IndexMap.listAll $ IndexMap.field (__ID :: WithID a -> ID a)
  (concat <$>) . for ids $ \ (_i,refs) -> case Set.toList refs of
    [r] -> fmap (: []) . deref =<< T.getDBRefM r
    []  -> return []
