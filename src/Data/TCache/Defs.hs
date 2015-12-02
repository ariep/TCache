{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{- | some internal definitions. To use default persistence, import
@Data.TCache.DefaultPersistence@ instead -}
module Data.TCache.Defs where

import           Control.Concurrent
import           Control.Concurrent.STM (STM, TVar)
import           Control.Exception as Exception
import           Control.Monad          (when, replicateM)
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.HashTable.IO as H
import           Data.IORef
import           Data.List              (elemIndices, isInfixOf, stripPrefix)
import qualified Data.Map          as Map
import           Data.Maybe             (fromJust, catMaybes)
import           Data.Typeable
import           System.Directory
  (
    createDirectoryIfMissing
  , getDirectoryContents
  , removeFile
  )
import           System.IO
  (
    openFile
  , IOMode(ReadMode)
  , hPutStrLn
  , hClose
  , hFileSize
  , stderr
  )
import           System.IO.Unsafe
import           System.IO.Error
import           System.Mem.Weak

--import Debug.Trace
--(!>) = flip trace

type AccessTime = Integer
type ModifTime  = Integer


data Status a
  = NotRead
  | DoNotExist
  | Exist a
  deriving (Typeable)

data Elem a
  = Elem !a !AccessTime !ModifTime
  deriving (Typeable)

type TPVar a
  = TVar (Status (Elem a))

data DBRef a
  = DBRef !String  !(TPVar a)
  deriving (Typeable)

castErr :: forall a b. (Typeable a, Typeable b) => String -> a -> b
castErr s a = case cast a of
  Just x  -> x
  Nothing -> error $
    "Type error: " ++ (show $ typeOf a) ++ " does not match "++ (show $ typeOf (undefined :: b))
    ++ "\nThis means that objects of these two types have the same key"
    ++ "\nor the retrieved object type is not the previously stored one for the same key."
    ++ "\n" ++ s

{- | Indexable is an utility class used to derive instances of IResource

Example:

@data Person= Person{ pname :: String, cars :: [DBRef Car]} deriving (Show, Read, Typeable)
data Car= Car{owner :: DBRef Person , cname:: String} deriving (Show, Read, Eq, Typeable)
@

Since Person and Car are instances of 'Read' ans 'Show', by defining the 'Indexable' instance
will implicitly define the IResource instance for file persistence:

@
instance Indexable Person where  key Person{pname=n} = \"Person \" ++ n
instance Indexable Car where key Car{cname= n} = \"Car \" ++ n
@
-}
class Indexable a where
  key :: a -> String

--instance IResource a => Indexable a where
--   key x = keyResource x


instance Indexable String where
  key = id

instance Indexable Int where
  key = show

instance Indexable Integer where
  key = show

instance Indexable () where
  key () = "void"


{- | Serialize is an alternative to the IResource class for defining persistence in TCache.
The deserialization must be as lazy as possible.
serialization/deserialization are not performance critical in TCache

Read, Show,  instances are implicit instances of Serializable

>    serialize  = pack . show
>    deserialize= read . unpack

Since write and read to disk of to/from the cache are not be very frequent
The performance of serialization is not critical.
-}
class Serializable a where
  serialize   :: a -> B.ByteString
  
  deserialize :: B.ByteString -> a
  deserialize = error "No deserialization defined for your data"
  
  deserialKey :: String -> B.ByteString -> a
  deserialKey _ v = deserialize v
  
  persist :: Proxy a -> Maybe Persist -- ^ `defaultPersist` if Nothing
  persist = const Nothing


type Key
  = String

type IResource a = (Typeable a, Indexable a, Serializable a)


-- there are two references to the DBRef here
-- The Maybe one keeps it alive until the cache releases it for *Resources
-- calls which does not reference dbrefs explicitly
-- The weak reference keeps the dbref alive until is it not referenced elsewere
data CacheElem
  = forall a. (IResource a, Typeable a) => CacheElem (Maybe (DBRef a)) (Weak (DBRef a))

type Ht
  = H.BasicHashTable String CacheElem
type Hts
  = Map.Map TypeRep Ht

-- Contains the hashtable and last sync time.
type Cache
  = IORef (Hts,Integer)

data CheckTPVarFlags
  = AddToHash
  | NoAddToHash

-- | Set the cache. This is useful for hot loaded modules that will update an existing cache. Experimental
-- setCache :: Cache -> IO ()
-- setCache ref = readIORef ref >>= \ch -> writeIORef refcache ch

-- | The cache holder. Established by default
-- refcache :: Cache
-- refcache = unsafePerformIO $ newCache >>= newIORef

-- | Creates a new cache. Experimental.
newCache  :: IO (Hts,Integer)
newCache = return (Map.empty,0)

data CMTrigger
  = forall a. (Typeable a) => CMTrigger !((DBRef a) -> Maybe a -> STM ())


-- | A persistence mechanism has to implement these primitives.
-- 'filePersist' is the default file persistence.
data Persist = Persist
  {
    readByKey   :: Key -> IO (Maybe B.ByteString) -- ^ read by key. It must be strict.
  , write       :: Key -> B.ByteString -> IO ()   -- ^ write. It must be strict.
  , delete      :: Key -> IO ()                   -- ^ delete
  , listByType  :: forall t. (Typeable t)
    => Proxy t -> IO [Key]                        -- ^ List keys of objects of the given type.
  , initialise  :: IO ()                          -- ^ Perform initialisation of this persistence store.
  , cmtriggers  :: IORef [(TypeRep, [CMTrigger])]
  , cache       :: Cache                          -- ^ Cached values.
  , persistName :: String                         -- ^ For showing.
  }

instance Show Persist where
  show p = persistName p

-- | Implements default persistence of objects in files with their keys as filenames,
-- inside the given directory.
filePersist :: FilePath -> IO Persist
filePersist dir = do
  t <- newIORef []
  c <- newCache >>= newIORef
  return $ Persist
    {
      readByKey   = defaultReadByKey dir
    , write       = defaultWrite dir
    , delete      = defaultDelete dir
    , listByType  = defaultListByType dir
    , initialise  = createDirectoryIfMissing True dir
    , cmtriggers  = t
    , cache       = c
    , persistName = "File persist in " ++ show dir
    }

defaultReadByKey :: FilePath -> String -> IO (Maybe B.ByteString)
defaultReadByKey dir k = handle handler $ do
  s <- readFileStrict $ dir ++ "/" ++ k
  return $ Just s -- `debug` ("read "++ filename)
 where
  handler :: IOError -> IO (Maybe B.ByteString)
  handler e
    | isAlreadyInUseError e = defaultReadByKey dir k                         
    | isDoesNotExistError e = return Nothing
    | otherwise             = if "invalid" `isInfixOf` ioeGetErrorString e
        then error $ "defaultReadByKey: " ++ show e
          ++ " defPath and/or keyResource are not suitable for a file path:\n" ++ k ++ "\""
        else defaultReadByKey dir k

defaultWrite :: FilePath -> String -> B.ByteString -> IO ()
defaultWrite dir k x = safeWrite (dir ++ "/" ++ k) x
safeWrite filename str = handle handler $ B.writeFile filename str   -- !> ("write "++filename)
 where
  handler e -- (e :: IOError)
    | isDoesNotExistError e = do
        createDirectoryIfMissing True $ take (1 + (last $ elemIndices '/' filename)) filename -- maybe the path does not exist
        safeWrite filename str
    | otherwise = if ("invalid" `isInfixOf` ioeGetErrorString e)
        then
          error  $ "defaultWriteResource: " ++ show e ++ " defPath and/or keyResource are not suitable for a file path: " ++ filename
        else do
          hPutStrLn stderr $ "defaultWriteResource: " ++ show e ++ " in file: " ++ filename ++ " retrying"
          safeWrite filename str
              
defaultDelete :: FilePath -> String -> IO ()
defaultDelete dir k = handle (handler filename) $ removeFile filename where
  filename = dir ++ "/" ++ k
  handler :: String -> IOException -> IO ()
  handler file e
    | isDoesNotExistError e = return ()  --`debug` "isDoesNotExistError"
    | isAlreadyInUseError e = do
        hPutStrLn stderr $ "defaultDelResource: busy in file: " ++ filename ++ " retrying"
        -- threadDelay 100000   --`debug`"isAlreadyInUseError"
        defaultDelete dir k  
    | otherwise = do
        hPutStrLn stderr $ "defaultDelResource: " ++ show e ++ " in file: " ++ filename ++ " retrying"
        -- threadDelay 100000     --`debug` ("otherwise " ++ show e)
        defaultDelete dir k

defaultListByType :: forall t. (Typeable t) => FilePath -> Proxy t -> IO [Key]
defaultListByType dir _ = do
  files <- getDirectoryContents dir
  return . catMaybes . map (stripPrefix $ typeString ++ "-") $ files
 where
  typeString = show (typeOf (undefined :: t))


-- | Strict read from file, needed for default file persistence
readFileStrict f = openFile f ReadMode >>= \ h -> readIt h `finally` hClose h where
  readIt h = do
    s   <- hFileSize h
    let n = fromIntegral s
    str <- B.hGet h n
    return str


readResourceByKey :: forall t. (Indexable t, Serializable t, Typeable t)
  => Persist -> Key -> IO (Maybe t)
readResourceByKey store k = readByKey store (typedFile pr k)
  >>= evaluate . fmap (deserialKey k)
 where
  pr = Proxy :: Proxy t

readResourcesByKey :: forall t. (Indexable t, Serializable t, Typeable t)
  => Persist -> [Key] -> IO [Maybe t]
readResourcesByKey store = mapM (readResourceByKey store)

writeResource :: forall t. (Indexable t, Serializable t, Typeable t)
  => Persist -> t -> IO ()
writeResource store s = write store (typedFile pr $ key s) $ serialize s
 where
  pr = Proxy :: Proxy t

delResource :: forall t. (Indexable t, Serializable t, Typeable t)
  => Persist -> t -> IO ()
delResource store s = delete store $ typedFile pr (key s)
 where
  pr = Proxy :: Proxy t

listResources :: forall t. (Serializable t, Indexable t, Typeable t)
  => Persist -> Proxy t -> IO [Key]
listResources = listByType

typedFile :: forall t. (Indexable t, Typeable t) => Proxy t -> Key -> FilePath
typedFile _ k = typeString ++ "-" ++ k where
  typeString = show $ typeOf (undefined :: t)
