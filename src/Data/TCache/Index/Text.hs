{-# LANGUAGE GADTs              #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE StandaloneDeriving #-}
module Data.TCache.Index.Text where

import qualified Data.Search.Results as Results
import           Data.TCache
import           Data.TCache.Defs
import           Data.TCache.Index
import           Data.Text     (Text)
import qualified Data.Text.Index as I

import           Data.Foldable (foldMap)
import           Data.Functor  ((<$>))
import           Data.Monoid   (Endo(Endo,appEndo))
import           Data.Ord      (Down)
import           Data.Typeable (Typeable)


data Field r where
  Fields :: (r -> [Text]) -> String -> Field r
  deriving (Typeable)

fields :: (r -> [Text]) -> Field r
fields f = Fields f ""

instance Indexable (Field r) where
  key (Fields _ s) = s

instance
  ( IResource r,Typeable r
  ) => Selector (Field r) where
  type Record   (Field r) = r
  type Property (Field r) = [Text]
  selector (Fields f _) = f

instance
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Serializable (I.Index Key)
  ) => Indexed (Field r) where
  
  type Index (Field r)
    = I.Index Key
  
  emptyIndex _ = I.empty
  addToIndex      (Fields _ _) ts r = appEndo e where
    e = foldMap (Endo . I.addDocument (keyObjDBRef r)) ts
  removeFromIndex (Fields _ _) _ts r = I.removeDocument $ keyObjDBRef r

lookup ::
  ( Indexed (Field r),IResource (LabelledIndex (Field r))
  ) => Field r -> Text -> DB (Results.T Key I.Weight Text)
lookup s t = flip I.lookupPhrase t <$> readIndex s
