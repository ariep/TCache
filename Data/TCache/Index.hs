{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Data.TCache.Index
  (
    Selector,Record,Property,selector
  , Indexed,Index,emptyIndex,addToIndex,removeFromIndex
  , readIndex
  , index
--   , find
  ) where

import           Data.TCache
import           Data.TCache.Defs

import           Data.Foldable (foldMap)
import           Data.Functor  ((<$>))
import           Data.Monoid   (Endo(Endo),appEndo)
import           Data.Typeable (Typeable,typeRep,Proxy(Proxy))
import qualified Data.Map       as Map
import           Data.Map      (Map)
import qualified Data.Set       as Set
import           Data.Set      (Set)


class
  ( IResource (Record s),Typeable (Record s)
  , Typeable (Property s)
  ) => Selector s where
  type Record s
  type Property s
  selector :: s -> Record s -> Property s
  type QueryType s
  type QueryType s = ()
class
  ( Selector s,Indexable s,Typeable s
  , IResource (Index s),Typeable (Index s)
  , Serializable (Record s),Indexable (Record s)
  , Eq (Property s)
  ) => Indexed s where
  type Index s
  emptyIndex :: s -> Index s
  addToIndex,removeFromIndex :: s -> Property s -> DBRef (Record s) -> Index s -> Index s
--   type Query s (k :: QueryType s)
--   type Result s k
--   type Result s k = Set (DBRef (Record s))
--   lookupIndex :: k -> s -> Query s k -> Index s -> Result s k

-- find :: (Indexed s) => k -> s -> Query s k -> STM (Result s k)
-- find k s c = lookupIndex k s c <$> readIndex s

-- | Register a trigger for indexing the values of the field passed as parameter.
-- The indexed field can be used to perform relational-like searches.
index :: forall s. (Indexed s) => s -> IO ()
index s = do
  atomically checkIndex
  addTrigger $ selectorIndex s
 where
  indexRef = getIndexDBRef s
  checkIndex = readDBRef indexRef >>= \case
    Just _  -> return ()
    Nothing -> do
      let tr = show (typeRep (Proxy :: Proxy (Record s  )))
      let tp = show (typeRep (Proxy :: Proxy (Property s)))
      safeIOToSTM . putStrLn
        $ "The index from " ++ tr ++ " to " ++ tp ++ " is not there; generating..."
      refs <- map getDBRef <$> safeIOToSTM (defListResources (Proxy :: Proxy (Record s)))
      objects <- mapM readDBRef refs
      let f = foldMap single (zip refs objects)
          single (ref,Just object) = Endo $ addToIndex s (selector s object) ref
          single (_  ,Nothing    ) = Endo id
          newIndex = appEndo f (emptyIndex s)
      writeDBRef indexRef newIndex

selectorIndex :: (Indexed s)
  => s -> DBRef (Record s) -> Maybe (Record s) -> STM ()
selectorIndex s oldRef maybeNew = do
  maybeOld <- readDBRef oldRef
  let indexChanges = case (maybeOld,maybeNew) of
        (Nothing ,Nothing ) -> Nothing
        (Just old,Just new) ->
          if selector s old == selector s new
            then Nothing
            else Just $ insertNew new . deleteOld old
        (Just old,Nothing ) -> Just $ deleteOld old
        (Nothing ,Just new) -> Just $ insertNew new
  case indexChanges of
    Nothing -> return ()
    Just f  -> do
      let indexRef = getIndexDBRef s
      Just oldIndex <- readDBRef indexRef
      writeDBRef indexRef $ f oldIndex
 where
  deleteOld old = removeFromIndex s (selector s old) oldRef
  insertNew new = addToIndex      s (selector s new) (getDBRef . key $ new)

-- We use the key of the selector as the key for the associated index.
getIndexDBRef :: forall s. (Indexed s) => s -> DBRef (Index s)
getIndexDBRef s = getDBRef $! key s 

readIndex :: (Indexed s) => s -> STM (Index s)
readIndex s = readDBRef (getIndexDBRef s) >>= \case
  Just i  -> return i
  Nothing -> error "Data.TCache.Index.readIndex: index not found. Did you register the index?"

{-

-- | implement the relational-like operators, operating on record fields
class RelationOps field1 field2 res | field1 field2 -> res  where
    (.==.) :: field1 -> field2 -> STM  res
    (.>.) :: field1 -> field2 ->  STM  res
    (.>=.):: field1 -> field2 ->  STM  res
    (.<=.) :: field1 -> field2 -> STM  res
    (.<.) :: field1 -> field2 ->  STM  res

-- Instance of relations betweeen fields and values
-- field .op. value
instance (Queriable reg a) => RelationOps (reg -> a) a  [DBRef reg] where
    (.==.) field value= do
       (_ ,_ ,dbrefs) <- getIndex field value
       return dbrefs

    (.>.)  field value= retrieve field value (>)
    (.<.)  field value= retrieve field value (<)
    (.<=.) field value= retrieve field value (<=)

    (.>=.) field value= retrieve field value (>=)

join:: (Queriable rec v, Queriable rec' v)
       =>(v->v-> Bool) -> (rec -> v) -> (rec' -> v) -> STM[([DBRef rec], [DBRef rec'])]
join op field1 field2 =do
  idxs   <- indexOf field1
  idxs' <- indexOf field2
  return $ mix  idxs  idxs'
  where
  opv (v, _ )(v', _)= v `op` v'
  mix    xs  ys=
      let zlist= [(x,y) |  x <- xs , y <- ys, x `opv` y]
      in map ( \(( _, xs),(_ ,ys)) ->(xs,ys)) zlist

type JoinData reg reg'=[([DBRef reg],[DBRef reg'])]

-- Instance of relations betweeen fields
-- field1 .op. field2
instance (Queriable reg a ,Queriable reg' a ) =>RelationOps (reg -> a) (reg' -> a)  (JoinData reg reg') where

    (.==.)= join (==)
    (.>.) = join (>)
    (.>=.)= join (>=)
    (.<=.)= join (<=)
    (.<.) = join (<)

infixr 5 .==., .>., .>=., .<=., .<.

class SetOperations set set'  setResult | set set' -> setResult where
  (.||.) :: STM set -> STM set' -> STM setResult
  (.&&.) :: STM set -> STM set' -> STM setResult


instance SetOperations  [DBRef a] [DBRef a] [DBRef a] where
    (.&&.) fxs fys= do
     xs <- fxs
     ys <- fys
     return $ intersect xs ys

    (.||.) fxs fys= do
     xs <- fxs
     ys <- fys
     return $ union xs ys

infixr 4 .&&.
infixr 3 .||.

instance SetOperations  (JoinData a a') [DBRef a] (JoinData a a') where
    (.&&.) fxs fys= do
     xss <- fxs
     ys <- fys
     return [(intersect xs ys, zs) | (xs,zs) <- xss]

    (.||.) fxs fys= do
     xss <- fxs
     ys <- fys
     return [(union xs ys, zs) | (xs,zs) <- xss]

instance SetOperations  [DBRef a] (JoinData a a')  (JoinData a a') where
    (.&&.) fxs fys=  fys .&&. fxs
    (.||.) fxs fys=  fys .||. fxs

instance SetOperations  (JoinData a a') [DBRef a'] (JoinData a a') where
    (.&&.) fxs fys= do
     xss <- fxs
     ys <- fys
     return [(zs,intersect xs ys) | (zs,xs) <- xss]

    (.||.) fxs fys= do
     xss <- fxs
     ys <- fys
     return [(zs, union xs ys) | (zs,xs) <- xss]


-- |  return all  the (indexed)  values which this field has and a DBRef pointer to the register
indexOf :: (Queriable reg a) => (reg -> a) -> STM [(a,[DBRef reg])]
indexOf selector= do
   let [one, two]= typeRepArgs $! typeOf selector
   let rindex= getDBRef $! keyIndex one two
   mindex <- readDBRef rindex
   case mindex of
     Just (Index index) -> return $ M.toList index;
     _ -> do
        let fields= show $ typeOf  selector
        error $ "the index for "++ fields ++" do not exist. At main, use \"Data.TCache.IdexQuery.index\" to start indexing this field"

retrieve :: Queriable reg a => (reg -> a) -> a -> (a -> a -> Bool) -> STM[DBRef reg]
retrieve field value op= do
   index <- indexOf field
   let higuer = map (\(v, vals) -> if op v value then  vals else [])  index
   return $ concat higuer

-- from a Query result, return the records, rather than the references
recordsWith
  :: (IResource a, Typeable a) =>
     STM [DBRef a] -> STM [ a]
recordsWith dbrefs= dbrefs >>= mapM readDBRef >>= return . catMaybes



class Select  selector a res | selector a -> res  where
  select :: selector -> a -> res


{-
instance (Select sel1 a res1, Select sel2 b res2 )
          => Select (sel1, sel2) (a , b) (res1, res2)  where
  select (sel1,sel2)  (x, y) = (select sel1 x, select sel2 y)
-}


instance (Typeable reg, IResource reg) =>  Select (reg -> a) (STM [DBRef reg])  (STM [a]) where
  select sel xs= return . map sel  =<< return . catMaybes =<< mapM readDBRef  =<< xs


instance  (Typeable reg, IResource reg,
          Select (reg -> a) (STM [DBRef reg])  (STM [a]),
          Select (reg -> b) (STM [DBRef reg])  (STM [b]) )
          =>  Select ((reg -> a),(reg -> b)) (STM [DBRef reg])  (STM [(a,b)])
          where
    select (sel, sel') xs= mapM (\x -> return (sel x, sel' x)) =<< return . catMaybes =<< mapM readDBRef  =<< xs

instance  (Typeable reg, IResource reg,
          Select (reg -> a) (STM [DBRef reg])  (STM [a]),
          Select (reg -> b) (STM [DBRef reg])  (STM [b]),
          Select (reg -> c) (STM [DBRef reg])  (STM [c]) )
          =>  Select ((reg -> a),(reg -> b),(reg -> c)) (STM [DBRef reg])  (STM [(a,b,c)])
          where
    select (sel, sel',sel'') xs= mapM (\x -> return (sel x, sel' x, sel'' x)) =<< return . catMaybes =<< mapM readDBRef  =<< xs


instance  (Typeable reg, IResource reg,
          Select (reg -> a) (STM [DBRef reg])  (STM [a]),
          Select (reg -> b) (STM [DBRef reg])  (STM [b]),
          Select (reg -> c) (STM [DBRef reg])  (STM [c]),
          Select (reg -> d) (STM [DBRef reg])  (STM [d]) )
          =>  Select ((reg -> a),(reg -> b),(reg -> c),(reg -> d)) (STM [DBRef reg])  (STM [(a,b,c,d)])
          where
    select (sel, sel',sel'',sel''') xs= mapM (\x -> return (sel x, sel' x, sel'' x, sel''' x)) =<< return . catMaybes =<< mapM readDBRef  =<< xs

-- for join's   (field1 op field2)

instance  (Typeable reg, IResource reg,
          Typeable reg', IResource reg',
          Select (reg -> a) (STM [DBRef reg])  (STM [a]),
          Select (reg' -> b) (STM [DBRef reg'])  (STM [b]) )
          =>  Select ((reg -> a),(reg' -> b)) (STM (JoinData reg reg')) (STM [([a],[b])])
          where
    select (sel, sel') xss = xss >>=  mapM select1
        where
        select1 (xs, ys) = do
         rxs <- return . map sel  =<< return . catMaybes  =<< mapM readDBRef  xs
         rys <- return .  map sel'  =<< return . catMaybes  =<< mapM readDBRef  ys
         return (rxs,rys)

-}