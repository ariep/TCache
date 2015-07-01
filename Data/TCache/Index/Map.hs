{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
module Data.TCache.Index.Map
  (
    Field(Fields)
  , field
  , lookup
  , lookupGT
  , lookupLT
  , listAll
  ) where

import           Data.TCache
import           Data.TCache.Defs
import           Data.TCache.Index

import           Data.Functor ((<$>))
import           Data.Functor.Identity (Identity(Identity))
import           Data.Foldable (Foldable,foldMap)
import qualified Data.Map       as Map
import           Data.Map   (Map)
import           Data.Monoid   (Endo(Endo),appEndo)
import qualified Data.Set       as Set
import           Data.Set   (Set)
import           Data.Typeable (Typeable)
import           Prelude hiding (lookup)


type RowSet r
  = Set (DBRef r)

data Field r f a where
  Fields :: (Foldable f) => (r -> f a) -> String -> Field r f a
  deriving (Typeable)

field :: (r -> a) -> String -> Field r Identity a
field f s = Fields (Identity . f) s

deriving instance Typeable Identity
instance (Eq a) => Eq (Identity a) where
  Identity x == Identity y = x == y

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
  , IResource (Map a (RowSet r))
  ) => Indexed (Field r f a) where
  
  type Index (Field r f a)
    = Map a (RowSet r)
  
  emptyIndex _ = Map.empty
  addToIndex      (Fields _ _) ps r = appEndo e where
    e = foldMap (\ p -> Endo $ Map.insertWith Set.union p $ Set.singleton r) ps
  removeFromIndex (Fields _ _) ps r = appEndo e where
    e = foldMap (\ p -> Endo $ Map.update (f . Set.delete r) p) ps
    f x = if Set.null x then Nothing else Just x
  
--   type Query (Field r f a) k = (Ordering,a)

lookup ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , IResource (Map a (RowSet r))
  ) => Field r f a -> a -> STM (RowSet r)
lookup s a = maybe Set.empty id . Map.lookup a <$> readIndex s

lookupGT ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , IResource (Map a (RowSet r))
  ) => Field r f a -> a -> STM (RowSet r)
lookupGT s a = do
  m <- readIndex s
  let (_,equal,greater) = Map.splitLookup a m
  return . Set.unions $ maybe [] (: []) equal ++ Map.elems greater

lookupLT ::
  ( Serializable r,Indexable r,IResource r,Typeable r
  , Ord a,Typeable a
  , Typeable f,Eq (f a)
  , IResource (Map a (RowSet r))
  ) => Field r f a -> a -> STM (RowSet r)
lookupLT s a = do
  m <- readIndex s
  let (smaller,equal,_) = Map.splitLookup a m
  return . Set.unions $ Map.elems smaller ++ maybe [] (: []) equal

listAll :: (Indexed (Field r f a)) => Field r f a -> STM [(a,RowSet r)]
listAll s = Map.assocs <$> readIndex s

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


maximumQ :: Queriable reg a => (reg -> a) -> STM (Maybe (a,[DBRef reg]))
maximumQ selector = do
   let [one, two] = typeRepArgs $! typeOf selector
   let rindex = getDBRef $! keyIndex one two
   mindex <- readDBRef rindex
   case mindex of
     Just (Index index) -> return $ fmap fst (M.maxViewWithKey index)
     _ -> do
        let fields = show $ typeOf selector
        error $ "the index for "++ fields ++" do not exist. At main, use \"Data.TCache.IdexQuery.index\" to start indexing this field"

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