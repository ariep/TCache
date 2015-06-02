{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
module Data.TCache.FullTextSearch
  (
    SearchStatics(..)
  , indexFTS
  , query
  
  , defaultSearchRankParameters
  , normalise
  
  , Proxy(Proxy)
  
  -- debugging
  , indexKey
  , IndexFTS
  ) where


import           Control.Applicative   ((<$>))
import           Data.Ix               (Ix)
import qualified Data.SearchEngine       as S
import qualified Data.SearchEngine.Types as S
import qualified Data.Serialize    as C
import           Data.Serialize.Text   ()
import           Data.TCache
import           Data.TCache.Defs
import qualified Data.TCache.ID    as ID
import qualified Data.Text         as Text
import           Data.Typeable         (Typeable,Proxy(Proxy),typeRep)
import           Data.Vector.Cereal    ()
import           GHC.Generics          (Generic)
import           System.Mem.StableName (makeStableName)


type Text
  = Text.Text

data IndexFTS doc key field feature
  = IndexFTS
    {
      searchEngine :: S.SearchEngine doc key field feature
    }
  deriving (Generic,Typeable)
instance
  (SearchStatics doc key field feature
  ,Ord key,C.Serialize key
  ,Ix field,C.Serialize field)
  => C.Serialize (IndexFTS doc key field feature) where

instance
  (SearchStatics doc key field feature
  ,Ord key,C.Serialize key
  ,Ix field,C.Serialize field)
  => C.Serialize (S.SearchEngine doc key field feature) where
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

class SearchStatics doc key field feature where
  config     :: S.SearchConfig doc key field feature
  rankParams :: Proxy doc -> Proxy key -> S.SearchRankParameters field feature

instance
  (SearchStatics doc key field feature
  ,Ord key,C.Serialize key
  ,C.Serialize field,Ix field)
  => Serializable (IndexFTS doc key field feature) where
  serialize   = C.encodeLazy
  deserialize = either err id . C.decodeLazy where
    err e = error $ "Data.TCache.FullTextSearch: decoding error: " ++ e
  setPersist  = const Nothing

instance (Typeable doc) => Indexable (IndexFTS doc key field feature) where
  key _ = "indexFTS-" ++ indexKey (Proxy :: Proxy doc)
--   defPath _ = ".tcachedata/index/"

instance
  (SearchStatics doc key field feature
  ,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,C.Serialize field
  ,Typeable feature)
  => IResource (IndexFTS doc key field feature) where
  keyResource       = key
  writeResource     = defWriteResource
  readResourceByKey = defReadResourceByKey
  delResource       = defDelResource

indexKey :: (Typeable doc) =>
  Proxy doc -> String
indexKey pd = show (typeRep pd)

-- | Trigger the indexation of list fields with elements convertible to Text.
indexFTS ::
  (SearchStatics doc key field feature
  ,IResource doc,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,Bounded field,C.Serialize field
  ,Typeable feature,Ix feature,Bounded feature)
  => S.SearchConfig doc key field feature
  -> S.SearchRankParameters field feature
  -> IO ()
indexFTS sc rp = do
  addTrigger (indexTrigger sc)
  let empty = IndexFTS { searchEngine = S.initSearchEngine sc rp }
  withResources [empty] $ init empty
 where
  init empty [Nothing] = [empty] -- Nothing present, so insert empty index.
  init _     [Just _ ] = []      -- Index present, so do not insert anything.

indexTrigger :: forall doc key field feature.
  (SearchStatics doc key field feature
  ,IResource doc,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,Bounded field,C.Serialize field
  ,Typeable feature,Ix feature,Bounded feature)
  => S.SearchConfig doc key field feature -> DBRef doc -> Maybe doc -> STM ()
indexTrigger sc dbref newValue = do
  oldValue <- readDBRef dbref
  modification <- case (oldValue,newValue) of
    (Nothing,Just x') -> return $ S.insertDoc x'
    (Just x ,Nothing) -> return $ S.deleteDoc (S.documentKey sc x)
    (Just x ,Just x') -> do
      st  <- unsafeIOToSTM . makeStableName $ x  -- test if field
      st' <- unsafeIOToSTM . makeStableName $ x' -- has changed
      return $ if st == st'
        then id
        else S.insertDoc x' . S.deleteDoc (S.documentKey sc x)
  modifyIndex
    (modification ::
         S.SearchEngine doc key field feature
      -> S.SearchEngine doc key field feature)

modifyIndex ::
  (SearchStatics doc key field feature
  ,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,C.Serialize field
  ,Typeable feature)
  => (S.SearchEngine doc key field feature -> S.SearchEngine doc key field feature)
  -> STM ()
modifyIndex f = writeDBRef r =<< return . maybe noIndex m =<< readDBRef r where
  r = indexDBRef
  m ix = ix { searchEngine = f (searchEngine ix) }

getIndex ::
  (SearchStatics doc key field feature
  ,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,C.Serialize field
  ,Typeable feature)
  => STM (IndexFTS doc key field feature)
getIndex = maybe noIndex id <$> readDBRef r where
  r = indexDBRef

indexDBRef :: forall doc key field feature.
  (SearchStatics doc key field feature
  ,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,C.Serialize field
  ,Typeable feature)
  => DBRef (IndexFTS doc key field feature)
indexDBRef = getDBRef . key $ (undefined :: IndexFTS doc key field feature)

noIndex :: IndexFTS doc key field feature
noIndex = error "Data.TCache.FullTextSearch.modifyIndex: index not found. Did you initialise?"

query :: forall doc key field feature.
  (SearchStatics doc key field feature
  ,Typeable doc
  ,Typeable key,Ord key,C.Serialize key
  ,Typeable field,Ix field,Bounded field,C.Serialize field
  ,Typeable feature,Ix feature,Bounded feature)
  => S.SearchConfig doc key field feature -- ^ field to search in
  -> Text                                 -- ^ text to search
  -> STM [key]
query _sc n = do
  ix :: IndexFTS doc key field feature <- getIndex
  return $ S.query (searchEngine ix) [n]

-- Search engine configuration

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
