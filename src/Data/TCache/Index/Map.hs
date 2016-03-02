{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
module Data.TCache.Index.Map
  (
    Fields(Fields)
  , Field
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

data Fields r f a where
  Fields :: (Foldable f) => (r -> f a) -> String -> Fields r f a
  deriving (Typeable)

type Field r a
  = Fields r Identity a

namedFields :: (Foldable f) => (r -> f a) -> String -> Fields r f a
namedFields = Fields

fields :: (Foldable f) => (r -> f a) -> Fields r f a
fields f = namedFields f ""

namedField :: (r -> a) -> String -> Field r a
namedField f s = Fields (Identity . f) s

field :: (r -> a) -> Field r a
field f = namedField f ""

deriving instance Typeable Identity

instance Indexable (Fields r f a) where
  key (Fields _ s) = s

instance
  ( IResource r,Typeable r
  , Typeable f
  , Typeable a
  ) => Selector (Fields r f a) where
  type Record   (Fields r f a) = r
  type Property (Fields r f a) = f a
  selector (Fields f _) = f

instance
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , Serializable (Map a (RowSet r))
  ) => Indexed (Fields r f a) where
  
  type Index (Fields r f a)
    = Map a (RowSet r)
  
  emptyIndex _ = Map.empty
  addToIndex      (Fields _ _) ps r = appEndo e where
    e = foldMap (\ p -> Endo $ Map.insertWith Set.union p $ Set.singleton $ keyObjDBRef r) ps
  removeFromIndex (Fields _ _) ps r = appEndo e where
    e = foldMap (\ p -> Endo $ Map.update (f . Set.delete (keyObjDBRef r)) p) ps
    f x = if Set.null x then Nothing else Just x

lookup ::
  ( Indexed (Fields r f a),Ord a,IResource (LabelledIndex (Fields r f a))
  ) => Fields r f a -> a -> DB (RowSet r)
lookup s a = maybe Set.empty id . Map.lookup a <$> readIndex s

lookupGT ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , Serializable (Map a (RowSet r))
  , IResource (LabelledIndex (Fields r f a))
  ) => Fields r f a -> a -> DB (RowSet r)
lookupGT s a = do
  m <- readIndex s
  let (_,equal,greater) = Map.splitLookup a m
  return . Set.unions $ maybe [] (: []) equal ++ Map.elems greater

lookupLT ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , Serializable (Map a (RowSet r))
  , IResource (LabelledIndex (Fields r f a))
  ) => Fields r f a -> a -> DB (RowSet r)
lookupLT s a = do
  m <- readIndex s
  let (smaller,equal,_) = Map.splitLookup a m
  return . Set.unions $ Map.elems smaller ++ maybe [] (: []) equal

listAll :: (Indexed (Fields r f a),IResource (LabelledIndex (Fields r f a)))
  => Fields r f a -> DB [(a,RowSet r)]
listAll s = Map.assocs <$> readIndex s

lookupAll :: forall r a.
  ( Indexed (Fields r Set a),Ord a
  , IResource (LabelledIndex (Fields r Set a))
  ) => Fields r Set a -> [a] -> DB [Key]
lookupAll s [] = error "Data.TCache.Index.Map.lookupAll: empty list of search parameters"
lookupAll s qs = do
  sized <- mapM (\ q -> (,) q . Set.size <$> lookup s q) qs
  let ((q,_) : rest) = sortBy (compare `on` snd) sized
  rs <- Set.toList <$> lookup s q
  foldM restrict rs $ map fst rest
 where
  restrict :: [Key] -> a -> DB [Key]
  restrict rs q = filterM
    (return . maybe False (Set.member q . selector s) <=< readDBRef <=< getDBRefM) rs

