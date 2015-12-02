{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{- | TCache is a transactional cache with configurable persistence that permits
STM transactions with objects that syncronize sincromous or asyncronously with
their user defined storages. Default persistence in files is provided by default

 TCache implements ''DBRef' 's . They are persistent STM references with a typical Haskell interface.
similar to TVars ('newDBRef', 'readDBRef', 'writeDBRef' etc) but with added. persistence
. DBRefs are serializable, so they can be stored and retrieved.
Because they are references, they point to other serializable registers.
This permits persistent mutable Inter-object relations.

Triggers in "Data.TCache.Triggers" are user defined hooks that are called back on register updates.
They are used internally for indexing.

"Data.TCache.Index" implements an straighforwards pure haskell type safe query language based
 on register field relations. This module must be imported separately.

"Data.TCache.Index.Text" add full text search and content search to the query language.

"Data.Persistent.Collection" implements a persistent, transactional collection with Queue interface as well as
 indexed access by key.

-}
module Data.TCache
  (
    -- * Inherited from 'Control.Concurrent.STM' and variations
    atomically
  , atomicallySync
  , STM
  , unsafeIOToSTM
  , safeIOToSTM
  
  -- * Operations with cached database references
{-|  @DBRefs@ are persistent cached database references in the STM monad
with read/write primitives, so the traditional syntax of Haskell STM references
can be used for  interfacing with databases. As expected, the DBRefs are transactional,
 because they operate in the STM monad.

A @DBRef@ is associated with its referred object through its key.
Since DBRefs are serializable, they can be elements of mutable cached objects themselves. They could point to other mutable objects
and so on, so DBRefs can act as \"hardwired\" relations from mutable objects
to other mutable objects in the database/cache. their referred objects are loaded, saved and flused
to and from the cache automatically depending on the cache handling policies and the access needs


@DBRefs@ are univocally identified by its pointed object keys, so they can be compared, ordered checked for equality so on.
The creation of a DBRef, trough 'getDBRef' is pure. This permits an efficient lazy access to the
 registers trouth their DBRefs by lazy marshalling of the register content on demand.

Example: Car registers have references to Person regiters

@
data Person= Person {pname :: String} deriving  (Show, Read, Eq, Typeable)
data Car= Car{owner :: DBRef Person , cname:: String} deriving (Show, Read, Eq, Typeable)
@


Here the Car register point to the Person register trough the owner field

To permit persistence and being refered with DBRefs, define the Indexable instance
for these two register types:

@
instance Indexable Person where key Person{pname= n} = "Person " ++ n
instance Indexable Car where key Car{cname= n} = "Car " ++ n
@

Now we create a DBRef to a Person whose name is \"Bruce\"

>>> let bruce =   getDBRef . key $ Person "Bruce" :: DBRef Person

>>> show bruce
>"DBRef \"Person bruce\""

>>> atomically (readDBRef bruce)
>Nothing

'getDBRef' is pure and creates the reference, but not the referred object;
To create both the reference and the DBRef, use 'newDBRef'.
Lets create two Car's and its two Car DBRefs with bruce as owner:

>>> cars <- atomically $  mapM newDBRef [Car bruce "Bat Mobile", Car bruce "Porsche"]

>>> print cars
>[DBRef "Car Bat Mobile",DBRef "Car Porsche"]

>>> carRegs<- atomically $ mapM readDBRef cars
> [Just (Car {owner = DBRef "Person bruce", cname = "Bat Mobile"})
> ,Just (Car {owner = DBRef "Person bruce", cname = "Porsche"})]

try to write with 'writeDBRef'

>>> atomically . writeDBRef bruce $ Person "Other"
>*** Exception: writeDBRef: law of key conservation broken: old , new= Person bruce , Person Other

DBRef's can not be written with objects of different keys

>>> atomically . writeDBRef bruce $ Person "Bruce"

>>> let Just carReg1= head carRegs

now from the Car register it is possible to recover the owner's register

>>> atomically $ readDBRef ( owner carReg1)
>Just (Person {pname = "bruce"})



DBRefs, once the pointed cached object is looked up in the cache and found at creation, they does
not perform any further cache lookup afterwards, so reads and writes from/to DBRefs are faster
than *Resource(s) calls, which perform  cache lookups everytime the object is accessed

DBRef's and @*Resource(s)@ primitives are completely interoperable. The latter operate implicitly with DBRef's

-}
  , DBRef
  , getDBRef
  , keyObjDBRef
  , newDBRef
  --, newDBRefIO
  , readDBRef
  , readDBRefs
  , writeDBRef
  , delDBRef
  
  -- * @IResource@ class
{- | cached objects must be instances of IResource.
Such instances can be implicitly derived through auxiliary classes for file persistence.
-}
  , IResource(..)
  
  -- * Trigger operations
{- | Trriggers are called just before an object of the given type is created, modified or deleted.
The DBRef to the object and the new value is passed to the trigger.
The called trigger function has two parameters: the DBRef being accesed
(which still contains the old value), and the new value.
If the content of the DBRef is being deleted, the second parameter is 'Nothing'.
if the DBRef contains Nothing, then the object is being created

Example:

Every time a car is added, or deleted, the owner's list is updated.
This is done by the user defined trigger addCar

@
 addCar pcar (Just(Car powner _ )) = addToOwner powner pcar
 addCar pcar Nothing  = readDBRef pcar >>= \\(Just car)-> deleteOwner (owner car) pcar

 addToOwner powner pcar=do
    Just owner <- readDBRef powner
    writeDBRef powner owner{cars= nub $ pcar : cars owner}

 deleteOwner powner pcar= do
   Just owner <- readDBRef powner
   writeDBRef powner owner{cars= delete  pcar $ cars owner}

 main= do
    'addTrigger' addCar
    putStrLn \"create bruce's register with no cars\"
    bruce \<- 'atomically' 'newDBRef' $ Person \"Bruce\" []
    putStrLn \"add two car register with \\"bruce\\" as owner using the reference to the bruces register\"
    let newcars= [Car bruce \"Bat Mobile\" , Car bruce \"Porsche\"]
    insert newcars
    Just bruceData \<- atomically $ 'readDBRef' bruce
    putStrLn \"the trigger automatically updated the car references of the Bruce register\"
    print . length $ cars bruceData
    print bruceData
@

gives:

> main
> 2
> Person {pname = "Bruce", cars = [DBRef "Car Porsche",DBRef "Car Bat Mobile"]}

-}
  , addTrigger

  -- * Cache control
{-- |

The mechanism for dropping elements from the cache is too lazy. `flushDBRef`, for example
just delete the data element  from the TVar, but the TVar node
remains attached to the table so there is no decrement on the number of elements.
The element is garbage collected unless you have a direct reference to the element, not the DBRef
Note that you can still have a valid reference to this element, but this element  is no longer
in the cache. The usual thing is that you do not have it, and the element will be garbage
collected (but still there will be a NotRead entry for this key!!!). If the DBRef is read again, the
TCache will go to permanent storage to retrieve it.

clear opertions such `clearsyncCache` does something similar:  it does not delete the
element from the cache. It just inform the garbage collector that there is no longer necessary to maintain
the element in the cache. So if the element has no other references (maybe you keep a
variable that point to that DBRef) it will be GCollected.
If this is not possible, it will remain in the cache and will be treated as such,
until the DBRef is no longer referenced by the program. This is done by means of a weak pointer

All these complications are necessary because the programmer can  handle DBRefs directly,
so the cache has no complete control of the DBRef life cycle, short to speak.

a DBRef can be in the states:

- `Exist`:  it is in the cache

- `DoesNotExist`: neither is in the cache neither in storage: it is like a cached "notfound" to
speed up repeated failed requests

- `NotRead`:  may exist or not in permanent storage, but not in the cache


In terms of Garbage collection it may be:



1 - pending garbage collection:  attached to the hashtable by means of a weak pointer: delete it asap

2 - cached: attached by a direct pointer and a weak pointer: It is being cached


clearsyncCache just pass elements from 2 to 1

--}
  , flushDBRef
  , flushKey
  , invalidateKey
  , flushAll
  -- , Cache
  -- , setCache
  -- , newCache
  --, refcache
  , syncCache
  , setConditions
  , clearSyncCache
  , numElems
  , syncWrite
  , SyncMode(..)
  , clearSyncCacheProc
  , defaultCheck
  -- * Other
  , onNothing
  ) where

import Data.TCache.Defs
import Data.TCache.Triggers

import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad   (when)
import           Data.Char       (isSpace)
import           Data.Functor    ((<$>))
import qualified Data.HashTable.IO as H
import           Data.IORef
import qualified Data.Map          as Map
import           Data.Maybe
import qualified Data.Serialize    as C
import           Data.Typeable
import           GHC.Conc
import           System.IO       (hPutStr,stderr)
import           System.IO.Unsafe
import           System.Mem
import           System.Mem.Weak
import           System.Time
          

--import Debug.Trace
--(!>) = flip trace



-- | Return the total number of DBRefs in the cache. For debug purposes.
-- This does not count the number of objects in the cache since many of the DBRef
-- may not have the pointed object loaded. It's O(n).
numElems :: Persist -> IO Int
numElems store = do
  (cache,_) <- readIORef $ cache store
  elems <- htsToList cache
  return $ length elems

deRefWeakSTM = unsafeIOToSTM . deRefWeak

--deleteFromCache :: (IResource a, Typeable a) => DBRef a -> IO ()
--deleteFromCache (DBRef k tv)=   do
--    (cache, _) <- readIORef refcache
--    H.delete cache k    -- !> ("delete " ++ k)

fixToCache :: (IResource a, Typeable a) => Persist -> DBRef a -> IO ()
fixToCache store dbref@(DBRef k _tv)= do
  (hts,_) <- readIORef $ cache store
  w <- mkWeakPtr dbref $ Just $ fixToCache store dbref
  htsInsert store hts k (CacheElem (Just dbref) w)
  return ()

-- | Return the reference value. If it is not in the cache, it is fetched
-- from the database.
readDBRef :: (IResource a, Typeable a) => Persist -> DBRef a -> STM (Maybe a)
readDBRef store dbref@(DBRef key tv) = readTVar tv >>= \case
  Exist (Elem x _ mt) -> do
    t <- unsafeIOToSTM timeInteger
    writeTVar tv . Exist $ Elem x t mt
    return $ Just x
  DoNotExist -> return Nothing
  NotRead    -> safeIOToSTM (readResourceByKey store key) >>= \case
    Nothing -> do
      writeTVar tv DoNotExist
      return Nothing
    Just x  -> do
      t <- unsafeIOToSTM timeInteger
      writeTVar tv $ Exist $ Elem x t (-1)
      return $ Just x

-- | Read multiple DBRefs in a single request using the new 'readResourcesByKey'
readDBRefs :: (IResource a, Typeable a) => Persist -> [DBRef a] -> STM [(Maybe a)]
readDBRefs store dbrefs = do
  let mf (DBRef key tv) = readTVar tv >>= \case
        Exist (Elem x _ mt) -> do
          t <- unsafeIOToSTM timeInteger
          writeTVar tv . Exist $ Elem x t mt
          return $ Right $ Just x
        DoNotExist -> return $ Right Nothing
        NotRead ->  return $ Left key
  inCache <- mapM mf dbrefs
  let pairs = foldr (\ pair@(x,dbr) xs -> case x of
        Left k -> pair : xs
        _      -> xs
        ) [] $ zip inCache dbrefs
  let (toReadKeys,dbrs) = unzip pairs
  let fromLeft (Left k) = k
      fromLeft _        = error "this will never happen"
  rs <- safeIOToSTM . readResourcesByKey store $ map fromLeft toReadKeys
  let processTVar (r,DBRef key tv) = case r of
        Nothing -> writeTVar tv DoNotExist
        Just x  -> do
          t <- unsafeIOToSTM timeInteger
          writeTVar tv $ Exist $ Elem x t (-1)
  mapM_ processTVar $ zip rs dbrs
  let mix (Right x : xs) ys       = x : mix xs ys
      mix (Left _  : xs) (y : ys) = y : mix xs ys
  return $ mix inCache rs

-- | Write in the reference a value
-- The new key must be the same as the old key of the previous object stored.
-- Otherwise, an error "law of key conservation broken" will be raised.
--
-- WARNING: the value to be written in the DBRef must be fully evaluated. Delayed evaluations at
-- serialization time can cause inconsistencies in the database.
-- In future releases this will be enforced.
writeDBRef :: (IResource a, Typeable a) => Persist -> DBRef a -> a -> STM ()
writeDBRef store dbref@(DBRef oldkey tv) x = x `seq` do
  let newkey = key x
  if newkey /= oldkey
    then error $ "writeDBRef: law of key conservation broken: old , new= " ++ oldkey ++ " , " ++ newkey
    else do
      applyTriggers store [dbref] [Just x]
      t <- unsafeIOToSTM timeInteger
      writeTVar tv $! Exist $! Elem x t t
      return ()


instance Show (DBRef a) where
  show (DBRef k _) = "DBRef \""++ k ++ "\""

instance Eq (DBRef a) where
  DBRef k _ == DBRef k' _ = k == k'

instance Ord (DBRef a) where
  compare (DBRef k _) (DBRef k' _) = compare k k'

-- instance (IResource a, Typeable a) => C.Serialize (DBRef a) where
--   get = getDBRef <$> C.get
--   put = C.put . keyObjDBRef

-- | Return the key of the object pointed to by the DBRef
keyObjDBRef :: DBRef a -> String
keyObjDBRef (DBRef k _) = k

-- | Get the reference to the object in the cache. If it does not exist, the reference is created empty.
-- Every execution of 'getDBRef' returns the same unique reference to this key,
-- so it can be safely considered pure. This is a property useful because deserialization
-- of objects with unused embedded DBRefs do not need to marshall them eagerly.
-- This also avoids unnecessary cache lookups of the pointed objects.
{-# NOINLINE getDBRef #-}
getDBRef :: forall a. (Typeable a, IResource a) => Persist -> Key -> DBRef a
getDBRef store key = unsafePerformIO $! getDBRef1 $! key where
 getDBRef1 :: (Typeable a, IResource a) => Key -> IO (DBRef a)
 getDBRef1 key = do
  (hts,_) <- readIORef $ cache store -- !> ("getDBRef "++ key)
  takeMVar getRefFlag
  r <- htsLookup (Proxy :: Proxy a) hts key
  case r of
    Just (CacheElem mdb w) -> do
      putMVar getRefFlag ()
      mr <- deRefWeak w
      case mr of
        Just dbref@(DBRef _ tv) ->
          case mdb of
            Nothing -> return $! castErr "1" dbref  -- !> "just"
            Just _  -> do
              htsInsert store hts key (CacheElem Nothing w) -- to notify when the DBREf leave its reference
              return $! castErr "2" dbref
        Nothing -> finalize w >> getDBRef1 key -- !> "finalize" -- the weak pointer has not executed his finalizer
    
    Nothing -> do
      tv <- newTVarIO NotRead                              -- !> "Nothing"
      dbref <- evaluate $ DBRef key tv
      w <- mkWeakPtr dbref . Just $ fixToCache store dbref
      htsInsert store hts key (CacheElem Nothing w)
      putMVar getRefFlag ()
      return dbref

getRefFlag = unsafePerformIO $ newMVar ()


-- | Create the object passed as parameter (if it does not exist) and
-- return its reference in the STM monad.
-- If an object with the same key already exists, it is returned as is
-- If not, the reference is created with the new value.
-- If you like to update in any case, use 'getDBRef' and 'writeDBRef' combined
-- if you  need to create the reference and the reference content, use 'newDBRef'
{-# NOINLINE newDBRef #-}
newDBRef :: (IResource a, Typeable a) => Persist -> a -> STM (DBRef a)
newDBRef store x = do
  let ref = getDBRef store $! key x
  mr <- readDBRef store ref
  case mr of
    Nothing -> writeDBRef store ref x >> return ref -- !> " write"
    Just r  -> return ref                           -- !> " non write"

-- | Delete the content of the DBRef form the cache and from permanent storage
delDBRef :: (IResource a, Typeable a) => Persist -> DBRef a -> STM ()
delDBRef store dbref@(DBRef k tv) = do
  mr <- readDBRef store dbref
  case mr of
   Just x -> do
     applyTriggers store [dbref] [Nothing]
     writeTVar tv DoNotExist
     safeIOToSTM . criticalSection saving $ delResource store x
   Nothing -> return ()

-- | Handles Nothing cases in a simpler way than runMaybeT.
-- it is used in infix notation. for example:
--
-- @result <- readDBRef ref \`onNothing\` error (\"Not found \"++ keyObjDBRef ref)@
--
-- or
--
-- @result <- readDBRef ref \`onNothing\` return someDefaultValue@
onNothing io onerr = do
  my <- io
  case my of
   Just y -> return y
   Nothing -> onerr

-- | Deletes the pointed object from the cache, not the database (see 'delDBRef')
-- useful for cache invalidation when the database is modified by other process
flushDBRef :: (IResource a, Typeable a) => DBRef a -> STM ()
flushDBRef (DBRef _ tv) = writeTVar tv NotRead

-- | flush the element with the given key
flushKey :: (Typeable a) => Persist -> Proxy a -> Key -> STM ()
flushKey store proxy key = do
  (hts,time) <- unsafeIOToSTM $ readIORef $ cache store
  c <- unsafeIOToSTM $ htsLookup proxy hts key
  case c of
    Nothing              -> return ()
    Just (CacheElem _ w) -> do
      mr <- unsafeIOToSTM $ deRefWeak w
      case mr of
        Just (DBRef k tv) -> writeTVar tv NotRead
        Nothing -> unsafeIOToSTM (finalize w) >> flushKey store proxy key

-- | label the object as not existent in database
invalidateKey :: (Typeable a) => Persist -> Proxy a -> Key -> STM ()
invalidateKey store proxy key = do
  (hts,time) <- unsafeIOToSTM $ readIORef $ cache store
  c <- unsafeIOToSTM $ htsLookup proxy hts key
  case c of
    Nothing               -> return ()
    Just  (CacheElem _ w) -> do
      mr <- unsafeIOToSTM $ deRefWeak w
      case mr of
        Just (DBRef k tv) -> writeTVar tv DoNotExist
        Nothing           -> unsafeIOToSTM (finalize w) >> flushKey store proxy key


-- | drops the entire cache.
flushAll :: Persist -> STM ()
flushAll store = do
  (hts,time) <- unsafeIOToSTM $ readIORef $ cache store
  elms <- unsafeIOToSTM $ htsToList hts
  mapM_ del elms
 where
  del (_,CacheElem _ w) = do
    mr <- unsafeIOToSTM $ deRefWeak w
    case mr of
      Just (DBRef _  tv) -> writeTVar tv NotRead
      Nothing -> unsafeIOToSTM (finalize w)


timeInteger = do
  TOD t _ <- getClockTime
  return t

releaseTPVars :: (IResource a, Typeable a) => Persist -> [a] -> Hts -> STM ()
releaseTPVars store rs hts = mapM_ (releaseTPVar store hts) rs

releaseTPVar :: forall a. (IResource a, Typeable a) => Persist -> Hts -> a -> STM ()
releaseTPVar store hts r = do
  c <- unsafeIOToSTM $ htsLookup (Proxy :: Proxy a) hts keyr
  case c of
    Just (CacheElem _ w) -> do
      mr <- unsafeIOToSTM $ deRefWeak w
      case mr of
        Nothing -> unsafeIOToSTM (finalize w) >> releaseTPVar store hts r
        Just dbref@(DBRef key tv) -> do
          applyTriggers store [dbref] [Just (castErr "4" r)]
          t <- unsafeIOToSTM  timeInteger
          writeTVar tv . Exist  $ Elem  (castErr "5" r) t t
    Nothing              -> do
      ti  <- unsafeIOToSTM timeInteger
      tvr <- newTVar NotRead
      dbref <- unsafeIOToSTM . evaluate $ DBRef keyr tvr
      applyTriggers store [dbref] [Just r]
      writeTVar tvr . Exist $ Elem r ti ti
      w <- unsafeIOToSTM . mkWeakPtr dbref $ Just $ fixToCache store dbref
      unsafeIOToSTM $ htsInsert store hts keyr
        (CacheElem (Just dbref) w) -- accessed and modified XXX
 where
  keyr = key r

delListFromHash :: forall a. (Typeable a,IResource a) => Hts -> [a] -> STM ()
delListFromHash hts xs = mapM_ del xs
 where
  del :: IResource a => a -> STM ()
  del x = do
    let keyx = key x
    mr <- unsafeIOToSTM $ htsLookup (Proxy :: Proxy a) hts keyx
    case mr of
      Nothing -> return ()
      Just (CacheElem _ w) -> do
        mr <- unsafeIOToSTM $ deRefWeak w
        case mr of
          Just dbref@(DBRef _ tv) -> writeTVar tv DoNotExist
          Nothing -> unsafeIOToSTM (finalize w) >> del x

updateListToHash :: Persist -> Hts -> [(String,CacheElem)] -> IO ()
updateListToHash store = mapM_ . uncurry . htsInsert store

htsInsert :: Persist -> Hts -> String -> CacheElem -> IO ()
htsInsert store hts k v = case selectCache tr hts of
  Nothing -> do
    ht <- H.new
    H.insert ht k v
    modifyIORef' (cache store) (\ (m,i) -> (Map.insert tr ht m,i))
  Just ht -> H.insert ht k v
 where
  tr :: TypeRep
  tr = case v of
    (CacheElem (_ :: Maybe (DBRef a)) _) -> typeRep (Proxy :: Proxy a)

htsLookup :: (Typeable a) => Proxy a -> Hts -> Key -> IO (Maybe CacheElem)
htsLookup p = maybe (const $ return Nothing) H.lookup . selectCache (typeRep p)

selectCache :: TypeRep -> Hts -> Maybe Ht
selectCache tr = Map.lookup tr

htsToList :: Hts -> IO [(Key,CacheElem)]
htsToList = fmap concat . sequence . map H.toList . Map.elems

-- | Start the thread that periodically call `clearSyncCache` to clean and writes on the persistent storage.
-- It is indirecly set by means of `syncWrite`, since it is higher level. I recommend to use the latter.
-- Otherwise, 'syncCache' or `clearSyncCache` or `atomicallySync` must be invoked explicitly or no persistence will exist.
-- Cache writes always save a coherent state.
clearSyncCacheProc ::
     Persist                      -- ^ Persistence store.
  -> Int                          -- ^ Number of seconds between checks. Objects not written to disk are written.
  -> (Integer -> Integer -> Integer -> Bool)  -- ^ The user-defined check-for-cleanup-from-cache for each object. 'defaultCheck' is an example
  -> Int                          -- ^ The max number of objects in the cache, if more, the  cleanup starts
  -> IO ThreadId           -- ^ Identifier of the thread created
clearSyncCacheProc store time check sizeObjects = forkIO clear
 where
  clear = do
    threadDelay $ time * 1000000
    handle ( \ (e :: SomeException)-> hPutStr stderr (show e) >> clear ) $ do
      clearSyncCache store check sizeObjects -- !>  "CLEAR"
      clear

criticalSection mv f = bracket
  (takeMVar mv)
  (putMVar mv)
  $ const $ f

-- | Force the atomic write of all cached objects modified since the last save into permanent storage.
-- Cache writes allways save a coherent state. As always, only the modified objects are written.
syncCache :: Persist -> IO ()
syncCache store = criticalSection saving $ do
  (hts,lastSync) <- readIORef $ cache store --`debug` "syncCache"
  t2 <- timeInteger
  elems <- htsToList hts
  (tosave,_,_) <- atomically $ extract elems lastSync
  save store tosave
  writeIORef (cache store) (hts,t2)


data SyncMode
  = Synchronous   -- ^ sync state to permanent storage when `atomicallySync` is invoked
  | Asynchronous   
      { frequency  :: Int                     -- ^ number of seconds between saves when asynchronous
      , check      :: (Integer-> Integer-> Integer-> Bool)  -- ^ The user-defined check-for-cleanup-from-cache for each object. 'defaultCheck' is an example
      , cacheSize  :: Int                     -- ^ size of the cache when async
      }
  | SyncManual               -- ^ use `syncCache` to write the state

tvSyncWrite = unsafePerformIO $ newIORef (Synchronous,Nothing)

-- | Specify the cache synchronization policy with permanent storage. See `SyncMode` for details.
syncWrite:: Persist -> SyncMode -> IO ()
syncWrite store mode = do
  (_,thread) <- readIORef tvSyncWrite
  when (isJust thread ) $ killThread . fromJust $ thread
  case mode of
    Synchronous -> modeWrite
    SyncManual  -> modeWrite
    Asynchronous time check maxsize -> do
      th <- clearSyncCacheProc store time check maxsize >> return ()
      writeIORef tvSyncWrite (mode,Just th)
 where
  modeWrite = writeIORef tvSyncWrite (mode,Nothing)


-- | Perform a synchronization of the cache with permanent storage once executed the STM transaction.
-- when 'syncWrite' policy is `Synchronous`.
atomicallySync :: Persist -> STM a -> IO a
atomicallySync store proc = do
  r <- atomically proc
  sync
  return r
 where
  sync = do
    (savetype,_) <- readIORef tvSyncWrite
    case savetype of
      Synchronous -> syncCache store
      _           -> return ()


-- | Saves the unsaved elems of the cache.
-- Cache writes always save a coherent state.
--  Unlike `syncCache` this call deletes some elems of the cache when the number of elems > @sizeObjects@.
--  The deletion depends on the check criteria, expressed by the first parameter.
--  'defaultCheck' is the one implemented to be passed by default. Look at it to understand the clearing criteria.
clearSyncCache :: Persist -> (Integer -> Integer -> Integer -> Bool) -> Int -> IO ()
clearSyncCache store check sizeObjects = criticalSection saving $ do
  (hts,lastSync) <- readIORef $ cache store
  t <- timeInteger
  elems <- htsToList hts
  (tosave,elems,size) <- atomically $ extract elems lastSync
  save store tosave
  when (size > sizeObjects) $
    forkIO (filtercache t hts lastSync elems) >> performGC
  writeIORef (cache store) (hts,t)
 where
  -- delete elems from the cache according with the checking criteria
  filtercache :: Integer -> Hts -> Integer -> [CacheElem] -> IO ()
  filtercache t hts lastSync elems = mapM_ filter elems where
    filter (CacheElem Nothing w) = return ()  -- alive because the dbref is being referenced elsewere
    filter (CacheElem (Just (DBRef key _ :: DBRef a)) w) = do
      mr <- deRefWeak w
      case mr of
        Nothing ->    finalize w
        Just (DBRef _  tv) -> atomically $ do
          r <- readTVar tv
          case r of
            Exist (Elem x lastAccess _ ) ->
              if check t lastAccess lastSync
                then do
                  unsafeIOToSTM $ htsInsert store hts key (CacheElem Nothing w)
                  writeTVar tv NotRead
                else return ()
            _    -> return ()

-- | This is a default cache clearance check. It forces to drop from the cache all the
-- elems not accesed since half the time between now and the last sync
-- if it returns True, the object will be discarded from the cache
-- it is invoked when the cache size exceeds the number of objects configured
-- in 'clearSyncCacheProc' or 'clearSyncCache'
defaultCheck
  :: Integer -- ^ current time in seconds
  -> Integer -- ^ last access time for a given object
  -> Integer -- ^ last cache syncronization (with the persisten storage)
  -> Bool    -- ^ return true for all the elems not accesed since half the time between now and the last sync
defaultCheck now lastAccess lastSync
  | lastAccess > halftime = False
  | otherwise             = True
  where
    halftime = now - (now - lastSync) `div` 2

refConditions = unsafePerformIO $ newIORef (return (),return ())

setConditions :: IO () -> IO () -> IO ()
-- ^ Establishes the procedures to call before and after saving with 'syncCache', 'clearSyncCache' or 'clearSyncCacheProc'. The postcondition of
-- database persistence should be a commit.
setConditions pre post = writeIORef refConditions (pre,post)

saving = unsafePerformIO $ newMVar False

save store tosave = do
  (pre,post) <- readIORef refConditions
  pre    -- !> (concatMap (\(Filtered x) -> keyResource x)tosave)
  mapM (\(Filtered x) -> writeResource store x) tosave
  post


data Filtered
  = forall a. (IResource a) => Filtered a

extract :: [(String,CacheElem)] -> Integer -> STM ([Filtered],[CacheElem],Int)
extract elems lastSave = filter1 [] [] (0:: Int)  elems
 where
  filter1 sav val n [] = return (sav, val, n)
  filter1 sav val n ((_, ch@(CacheElem mybe w)) : rest) = do
      mr <- unsafeIOToSTM $ deRefWeak w
      case mr of
        Nothing -> unsafeIOToSTM (finalize w) >> filter1 sav val n rest
        Just (DBRef key tvr) ->
         let tofilter = case mybe of
               Just _  -> ch : val
               Nothing -> val
         in do
           r <- readTVar tvr
           case r of
             Exist (Elem r _ modTime) ->
               if (modTime >= lastSave)
                 then filter1 (Filtered r : sav) tofilter (n+1) rest
                 else filter1 sav tofilter (n+1) rest -- !> ("rejected->" ++ keyResource r)
             _ -> filter1 sav tofilter (n+1) rest


-- | Assures that the IO computation finalizes no matter if the STM transaction
-- is aborted or retried. The IO computation run in a different thread.
-- The STM transaction wait until the completion of the IO procedure (or retry as usual).
--
-- It can be retried if the embedding STM computation is retried
-- so the IO computation must be idempotent.
-- Exceptions are bubbled up to the STM transaction
safeIOToSTM :: IO a -> STM a
safeIOToSTM req = unsafeIOToSTM $ do
  tv   <- newEmptyMVar
  forkIO $ (req >>= putMVar tv . Right)
          `Control.Exception.catch`
          (\(e :: SomeException) -> putMVar tv $ Left e)
  r <- takeMVar tv
  case r of
    Right x -> return x
    Left e  -> throw e
