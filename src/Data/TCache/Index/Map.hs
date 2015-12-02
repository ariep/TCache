{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
module Data.TCache.Index.Map
  (
    Field(Fields)
  , field
  , namedField
  , fields
  , namedFields
  , lookup
  , lookupGT
  , lookupLT
  , listAll
  , lookupAll
  ) where

import           Data.TCache
import           Data.TCache.Defs
import           Data.TCache.Index

import           Control.Monad         (foldM,filterM,(<=<))
import           Data.Function         (on)
import           Data.Functor          ((<$>))
import           Data.Functor.Classes  ()
import           Data.Functor.Identity (Identity(Identity))
import           Data.Foldable         (Foldable,foldMap)
import           Data.List             (sortBy)
import qualified Data.Map       as Map
import           Data.Map              (Map)
import           Data.Monoid           (Endo(Endo),appEndo)
import qualified Data.Set       as Set
import           Data.Set              (Set)
import           Data.Typeable         (Typeable)
import           Prelude hiding (lookup)


type RowSet r
  = Set Key

data Field r f a where
  Fields :: (Foldable f) => (r -> f a) -> String -> Field r f a
  deriving (Typeable)

namedFields :: (Foldable f) => (r -> f a) -> String -> Field r f a
namedFields = Fields

fields :: (Foldable f) => (r -> f a) -> Field r f a
fields f = namedFields f ""

namedField :: (r -> a) -> String -> Field r Identity a
namedField f s = Fields (Identity . f) s

field :: (r -> a) -> Field r Identity a
field f = namedField f ""

deriving instance Typeable Identity

instance Indexable (Field r f a) where
  key (Fields _ s) = s

instance
  ( IResource r,Typeable r
  , Typeable f
  , Typeable a
  ) => Selector (Field r f a) where
  type Record   (Field r f a) = r
  type Property (Field r f a) = f a
  selector (Fields f _) = f

instance
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , Serializable (Map a (RowSet r))
  ) => Indexed (Field r f a) where
  
  type Index (Field r f a)
    = Map a (RowSet r)
  
  emptyIndex _ = Map.empty
  addToIndex      (Fields _ _) ps r = appEndo e where
    e = foldMap (\ p -> Endo $ Map.insertWith Set.union p $ Set.singleton $ keyObjDBRef r) ps
  removeFromIndex (Fields _ _) ps r = appEndo e where
    e = foldMap (\ p -> Endo $ Map.update (f . Set.delete (keyObjDBRef r)) p) ps
    f x = if Set.null x then Nothing else Just x

lookup ::
  ( Indexed (Field r f a),Ord a,IResource (LabelledIndex (Field r f a))
  ) => Persist -> Field r f a -> a -> STM (RowSet r)
lookup store s a = maybe Set.empty id . Map.lookup a <$> readIndex store s

lookupGT ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , Serializable (Map a (RowSet r))
  , IResource (LabelledIndex (Field r f a))
  ) => Persist -> Field r f a -> a -> STM (RowSet r)
lookupGT store s a = do
  m <- readIndex store s
  let (_,equal,greater) = Map.splitLookup a m
  return . Set.unions $ maybe [] (: []) equal ++ Map.elems greater

lookupLT ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , Serializable (Map a (RowSet r))
  , IResource (LabelledIndex (Field r f a))
  ) => Persist -> Field r f a -> a -> STM (RowSet r)
lookupLT store s a = do
  m <- readIndex store s
  let (smaller,equal,_) = Map.splitLookup a m
  return . Set.unions $ Map.elems smaller ++ maybe [] (: []) equal

listAll :: (Indexed (Field r f a),IResource (LabelledIndex (Field r f a)))
  => Persist -> Field r f a -> STM [(a,RowSet r)]
listAll store s = Map.assocs <$> readIndex store s

lookupAll :: forall r a.
  ( Indexed (Field r Set a),Ord a
  , IResource (LabelledIndex (Field r Set a))
  ) => Persist -> Field r Set a -> [a] -> STM [Key]
lookupAll _     s [] = error "Data.TCache.Index.Map.lookupAll: empty list of search parameters"
lookupAll store s qs = do
  sized <- mapM (\ q -> (,) q . Set.size <$> lookup store s q) qs
  let ((q,_) : rest) = sortBy (compare `on` snd) sized
  rs <- Set.toList <$> lookup store s q
  foldM restrict rs $ map fst rest
 where
  restrict :: [Key] -> a -> STM [Key]
  restrict rs q = filterM (return . maybe False (Set.member q . selector s) <=< readDBRef store . getDBRef store) rs

