{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Data.TCache.Index.FullTextSearch
  (
    FTS(FTS)
  , SearchStatics(..)
  , query
  , queryAutosuggest
  
  , defaultSearchRankParameters
  , normalise
  
  , Proxy(Proxy)
  ) where

import           Data.TCache
import           Data.TCache.Defs
import qualified Data.TCache.ID    as ID
import           Data.TCache.Index

import           Control.Applicative   ((<$>))
import           Data.Ix               (Ix)
import qualified Data.SearchEngine       as S
import qualified Data.SearchEngine.Types as S
import qualified Data.Serialize    as C
import           Data.Serialize.Text   ()
import qualified Data.Text         as Text
import           Data.Typeable         (Typeable,Proxy(Proxy),typeRep)
import           Data.Vector.Cereal    ()
import           GHC.Generics          (Generic)
import           System.Mem.StableName (makeStableName)


type Text
  = Text.Text

-- type FTS doc field feature
--   = (S.SearchConfig doc (DBRef doc) field feature,S.SearchRankParameters field feature)

data FTS doc field feature
  = FTS
  deriving (Typeable)

instance Indexable (FTS doc field feature) where
  key _ = ""

instance (IResource doc,Typeable doc)
  => Selector (FTS doc field feature) where
  type Record   (FTS doc field feature) = doc
  type Property (FTS doc field feature) = doc
  selector _ = id
instance
  ( SearchStatics doc (DBRef doc) field feature
  , IResource doc,Serializable doc,Indexable doc,Eq doc,Typeable doc
  , C.Serialize field,Ix field,Bounded field,Typeable field
  , Ix feature,Bounded feature,Typeable feature
  ) => Indexed (FTS doc field feature) where
  type Index (FTS doc field feature)
    = S.SearchEngine doc (DBRef doc) field feature
  emptyIndex _ = S.initSearchEngine config
    (rankParams (Proxy :: Proxy doc) (Proxy :: Proxy (DBRef doc)) :: S.SearchRankParameters field feature)
  addToIndex      _ doc _dbref = S.insertDoc doc
  removeFromIndex _ doc _dbref = S.deleteDoc
    (S.documentKey (config :: S.SearchConfig doc (DBRef doc) field feature) doc)

query :: 
  ( IResource doc,Indexable doc,Serializable doc,Eq doc,Typeable doc
  , Bounded field,C.Serialize field,Ix field,Typeable field
  , Ix feature,Bounded feature,Typeable feature
  , SearchStatics doc (DBRef doc) field feature
  , IResource (LabelledIndex (FTS doc field feature))
  ) => FTS doc field feature -> [S.Term] -> STM [DBRef doc]
query fts qs = do
  se <- readIndex fts
  return $ S.query se qs

queryAutosuggest ::
  ( IResource doc,Indexable doc,Serializable doc,Eq doc,Typeable doc
  , Bounded field,C.Serialize field,Ix field,Typeable field
  , Ix feature,Bounded feature,Typeable feature
  , SearchStatics doc (DBRef doc) field feature
  , IResource (LabelledIndex (FTS doc field feature))
  ) => FTS doc field feature -> [S.Term] -> S.Term -> STM ([(S.Term,Float)],[(DBRef doc,Float)])
queryAutosuggest fts qs a = do
  se <- readIndex fts
  return $ S.queryAutosuggest se S.NoFilter qs a

instance
  ( SearchStatics doc key field feature
  , Ord key,C.Serialize key
  , Ix field,C.Serialize field
  ) => C.Serialize (S.SearchEngine doc key field feature) where
  put se = C.put (S.searchIndex se) >> C.put (S.sumFieldLengths se)
  get = do
    si <- C.get
    sfl <- C.get
    let temp = S.SearchEngine
          {
            S.searchIndex      = si
          , S.searchConfig     = config
          , S.searchRankParams = rankParams (Proxy :: Proxy doc) (Proxy :: Proxy key)
          , S.sumFieldLengths  = sfl
          , S.bm25Context      = error "bm25Context undefined"
          }
    return $ S.cacheBM25Context temp
instance (Ord key,C.Serialize key)
  => C.Serialize (S.SearchIndex key field feature) where
instance (C.Serialize key)
  => C.Serialize (S.DocInfo key field feature) where
instance C.Serialize S.DocId where
instance C.Serialize S.DocIdSet where
instance C.Serialize (S.DocFeatVals feature) where
instance C.Serialize (S.DocTermIds field) where
instance C.Serialize S.TermBag where
instance C.Serialize S.TermInfo where
instance C.Serialize S.TermId where
instance C.Serialize S.TermIdInfo where

instance
  ( SearchStatics doc key field feature
  , Ord key,C.Serialize key
  , C.Serialize field,Ix field)
  => Serializable (S.SearchEngine doc key field feature) where
  serialize   = C.encodeLazy
  deserialize = either err id . C.decodeLazy where
    err e = error $ "Data.TCache.FullTextSearch: decoding error: " ++ e
  setPersist  = const Nothing

instance (Typeable doc) => Indexable (S.SearchEngine doc key field feature) where
  key = const ""

instance
  ( SearchStatics doc key field feature
  , Typeable doc
  , Typeable key,Ord key,C.Serialize key
  , Typeable field,Ix field,C.Serialize field
  , Typeable feature
  ) => IResource (S.SearchEngine doc key field feature) where
  keyResource       = key
  writeResource     = defWriteResource
  readResourceByKey = defReadResourceByKey
  delResource       = defDelResource

-- Search engine configuration

class SearchStatics doc key field feature where
  config     :: S.SearchConfig doc key field feature
  rankParams :: Proxy doc -> Proxy key -> S.SearchRankParameters field feature

normalise :: (Text -> Text)
  -> S.SearchConfig doc key field feature
  -> S.SearchConfig doc key field feature
normalise f sc = sc
  {
    S.extractDocumentTerms  = \ doc field -> map f (S.extractDocumentTerms sc doc field)
  , S.transformQueryTerm    = \ tok field -> f $ S.transformQueryTerm sc tok field
  }

defaultSearchRankParameters :: S.SearchRankParameters () S.NoFeatures
defaultSearchRankParameters = S.SearchRankParameters
  {
    S.paramK1                         = 1.5
  , S.paramB                          = \ () -> 1.0
  , S.paramFieldWeights               = \ () -> 1.0
  , S.paramFeatureWeights             = S.noFeatures
  , S.paramFeatureFunctions           = S.noFeatures
  , S.paramResultsetSoftLimit         = 20
  , S.paramResultsetHardLimit         = 40
  , S.paramAutosuggestPrefilterLimit  = 20
  , S.paramAutosuggestPostfilterLimit = 10
  }
