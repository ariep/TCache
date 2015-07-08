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
  ( T.IResource (WithID a),T.PersistIndex (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set (Ref a)))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) Identity (ID a)))
  ) => a -> T.STM (WithID a)
new x = loop where
  loop = do
    i <- ID <$> T.unsafeIOToSTM (randomText 8)
    existing <- IndexMap.lookup (IndexMap.field (__ID :: WithID a -> ID a)) i
    if Set.null existing
      then return $ WithID i x
      else loop

randomText :: Int -> IO Text
randomText l = pack . take l . randomRs ('a','z') <$> newStdGen

type Ref a
  = T.DBRef (WithID a)

newRef ::
  ( T.IResource (WithID a),T.PersistIndex (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set (Ref a)))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) Identity (ID a)))
  ) => a -> T.STM (Ref a)
newRef x = T.newDBRef =<< new x

delete :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.STM ()
delete = T.delDBRef

update :: (T.IResource (WithID a),Typeable a)
  => Ref a -> a -> T.STM (Maybe ())
update r x = T.readDBRef r >>= \case
  Just (WithID i _) -> Just <$> T.writeDBRef r (WithID i x)
  Nothing           -> return Nothing

ref :: forall a. (T.IResource (WithID a),Typeable a)
  => ID a -> Ref a
ref i = T.getDBRef $ T.keyResource (WithID i undefined :: WithID a)

deref :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.STM (WithID a)
deref r = maybe err id <$> T.readDBRef r where
  err = error $ "Data.TCache.ID.deref: ID " ++ show r ++ " does not occur in database"

refLens :: (T.IResource (WithID a),Typeable a) =>
  MLens T.STM (Ref a) (Maybe ()) a a
refLens = mkMLens (fmap _object . deref) update

listWithID :: forall a.
  ( T.IResource (WithID a),T.PersistIndex (WithID a),T.Serializable (WithID a)
  , Typeable a
  , T.Serializable (Map.Map (ID a) (Set.Set (Ref a)))
  , T.IResource (T.LabelledIndex (IndexMap.Field (WithID a) Identity (ID a)))
  )
  => T.STM [WithID a]
listWithID = do
  ids <- IndexMap.listAll $ IndexMap.field (__ID :: WithID a -> ID a)
  (concat <$>) . for ids $ \ (_i,refs) -> case Set.toList refs of
    [r] -> (: []) <$> deref r
    []  -> return []
