{-# LANGUAGE ExistentialQuantification, DeriveDataTypeable, BangPatterns #-}
module Data.TCache.Triggers
  (
    DBRef(..)
  , Elem(..)
  , Status(..)
  , addTrigger
  , applyTriggers
  ) where

import Data.TCache.Defs

import Control.Concurrent.STM
import Data.IORef
import Data.Maybe (maybeToList,catMaybes,fromJust)
import Data.List  (nubBy)
import Data.Typeable
import GHC.Conc   (STM,unsafeIOToSTM)
import System.IO.Unsafe
import Unsafe.Coerce

import Debug.Trace

newtype TriggerType a
  = TriggerType (DBRef a -> Maybe a -> STM ())
  deriving (Typeable)

{- | Add an user defined trigger to the list of triggers.
Triggers are called just before an object of the given type is created, modified or deleted.
The DBRef to the object and the new value is passed to the trigger.
The called trigger function has two parameters: the DBRef being accesed
(which still contains the old value), and the new value.
If the DBRef is being deleted, the second parameter is 'Nothing'.
if the DBRef contains Nothing, then the object is being created
-}
addTrigger :: (Typeable a) => Persist -> ((DBRef a) -> Maybe a -> STM ()) -> IO ()
addTrigger store t = do
  let triggers = cmtriggers store
  map <- readIORef triggers
  writeIORef triggers $
    let ts = mbToList $ lookup atype map
      in nubByType $ (atype, CMTrigger t : ts) : map
 where
  nubByType = nubBy (\ (t,_) (t',_) -> t == t')
  (_,(atype : _)) = splitTyConApp  . typeOf $ TriggerType t

mbToList :: Maybe [x] -> [x]
mbToList = maybe [] id

-- | internally called when a DBRef is modified/deleted/created
applyTriggers:: (Typeable a) => Persist -> [DBRef a] -> [Maybe a] -> STM ()
applyTriggers store [] _ = return ()
applyTriggers store dbrfs mas = do
  map <- unsafeIOToSTM $ readIORef $ cmtriggers store
  let ts = mbToList $ lookup (typeOf $ fromJust (head mas)) map
  mapM_ f ts
 where
  f t = mapM2_ (f1 t) dbrfs mas

  f1 :: (Typeable a) => CMTrigger -> DBRef a -> Maybe a -> STM ()
  f1 (CMTrigger t) dbref ma = (unsafeCoerce t) dbref ma

mapM2_ _ [] _ = return ()
mapM2_ f (x : xs) (y : ys) = f x y >> mapM2_ f xs ys
