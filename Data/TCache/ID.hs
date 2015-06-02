{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
module Data.TCache.ID
  (
    ID(ID)
  , WithID(WithID,object,_id)
  , new
  , Ref
  , newRef
  , lookup
  , deref
  , maybeDeref
  , selector
  , listWithID
  , delete
  , update
  ) where

import qualified Data.TCache      as T
import qualified Data.TCache.Defs as T
import qualified Data.TCache.IndexQuery as T

import           Control.Applicative ((<$>))
import qualified Data.Serialize   as C
import           Data.Serialize.Text ()
import           Data.Text           (Text,pack)
import           Data.Traversable    (for)
import           Data.Typeable       (Typeable)
import           GHC.Generics        (Generic)
import           Prelude hiding (lookup)
import           System.Random       (newStdGen,randomRs)


newtype ID a
  = ID Text
  deriving (Eq,Ord,Generic,Typeable,Show,Read)
instance C.Serialize (ID a) where

data WithID a
  = WithID
    {
      _id :: ID a
    , object :: a
    }
  deriving (Generic,Typeable,Show)

new :: forall a. (T.IResource (WithID a),T.PersistIndex (WithID a),Typeable a)
  => a -> T.STM (WithID a)
new x = loop where
  loop = do
    i <- ID <$> T.unsafeIOToSTM (randomText 8)
    existing <- (selector :: WithID a -> ID a) T..==. i
    case existing of
      [] -> return $ WithID i x
      _  -> loop

randomText :: Int -> IO Text
randomText l = pack . take l . randomRs ('a','z') <$> newStdGen

selector :: WithID a -> ID a
selector (WithID i _) = i

newRef :: (T.IResource (WithID a),T.PersistIndex (WithID a),Typeable a)
  => a -> T.STM (Ref a)
newRef x = T.newDBRef =<< new x

type Ref a
  = T.DBRef (WithID a)

lookup :: forall a. (T.IResource (WithID a),Typeable a)
  => ID a -> Ref a
lookup i = T.getDBRef $ T.keyResource (WithID i undefined :: WithID a)

deref :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.STM a
deref r = maybe err id <$> maybeDeref r where
  err = error "Data.TCache.ID.deref: ID does not occur in database"

maybeDeref :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.STM (Maybe a)
maybeDeref r = T.readDBRef r >>= \case
  Just (WithID _ x) -> return $ Just x
  Nothing           -> return Nothing

listWithID :: forall a. (T.IResource (WithID a),T.PersistIndex (WithID a),Typeable a)
  => T.STM [WithID a]
listWithID = do
  ids <- T.indexOf (selector :: WithID a -> ID a)
  (concat <$>) . for ids $ \ (i,refs) -> case refs of
    [r] -> (: []) . WithID i <$> deref r
    []  -> return []

delete :: (T.IResource (WithID a),Typeable a)
  => Ref a -> T.STM ()
delete = T.delDBRef

update :: (T.IResource (WithID a),Typeable a)
  => Ref a -> a -> T.STM (Maybe ())
update r x = do
  result <- T.readDBRef r
  case result of
    Just (WithID i _) -> Just <$> T.writeDBRef r (WithID i x)
    Nothing           -> return Nothing
