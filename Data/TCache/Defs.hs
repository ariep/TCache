{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}
{- | some internal definitions. To use default persistence, import
@Data.TCache.DefaultPersistence@ instead -}
module Data.TCache.Defs where

import Data.TCache.IResource

import           Control.Concurrent
import           Control.Concurrent.STM (TVar)
import           Control.Exception as Exception
import           Control.Monad          (when,replicateM)
import           Data.IORef
import           Data.List              (elemIndices,isInfixOf,stripPrefix)
import           Data.Maybe             (fromJust,catMaybes)
import           Data.Typeable
import           System.Directory
  (
    createDirectoryIfMissing
  , getDirectoryContents
  , removeFile
  )
import           System.IO
import           System.IO.Unsafe
import           System.IO.Error

import qualified Data.ByteString.Lazy.Char8 as B

--import Debug.Trace
--(!>) = flip trace

type AccessTime = Integer
type ModifTime  = Integer


data Status a= NotRead | DoNotExist | Exist a deriving Typeable

data Elem a= Elem !a !AccessTime !ModifTime   deriving Typeable

type TPVar a=   TVar (Status(Elem a))

data DBRef a= DBRef !String  !(TPVar a)  deriving Typeable

castErr :: forall a b. (Typeable a,Typeable b) => String -> a -> b
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
    key:: a -> String
    defPath :: a -> String       -- ^ additional extension for default file paths.
                                -- IMPORTANT:  defPath must depend on the datatype, not the value (must be constant). Default is ".tcachedata/"
    defPath =  const ".tcachedata/"

--instance IResource a => Indexable a where
--   key x= keyResource x


instance Indexable String where
  key= id

instance Indexable Int where
  key= show

instance Indexable Integer where
  key= show


instance Indexable () where
  key _= "void"


{- | Serialize is an alternative to the IResource class for defining persistence in TCache.
The deserialization must be as lazy as possible.
serialization/deserialization are not performance critical in TCache

Read, Show,  instances are implicit instances of Serializable

>    serialize  = pack . show
>    deserialize= read . unpack

Since write and read to disk of to/from the cache are not be very frequent
The performance of serialization is not critical.
-}
class Serializable a  where
  serialize   :: a -> B.ByteString
  deserialize :: B.ByteString -> a
  deserialize= error "No deserialization defined for your data"
  deserialKey :: String -> B.ByteString -> a
  deserialKey _ v= deserialize v
  setPersist  :: a -> Maybe Persist              -- ^ `defaultPersist` if Nothing
  setPersist =  const Nothing

-- |  Used by IndexQuery for index persistence(see "Data.TCache.IndexQuery".
class PersistIndex a where
   persistIndex :: a -> Maybe Persist


type Key
  = String

-- | A persistence mechanism has to implement these primitives.
-- 'filePersist' is the default file persistence.
data Persist = Persist
  {
    readByKey   :: Key -> IO (Maybe B.ByteString) -- ^ read by key. It must be strict.
  , write       :: Key -> B.ByteString -> IO ()   -- ^ write. It must be strict.
  , delete      :: Key -> IO ()                   -- ^ delete
  , listByType  :: (Typeable t)
    => FilePath -> Proxy t -> IO [Key]            -- ^ list key of objects of the given type.
  }

-- | Implements default default-persistence of objects in files with their keys as filenames
filePersist = Persist
  {
    readByKey  = defaultReadByKey
  , write      = defaultWrite
  , delete     = defaultDelete
  , listByType = defaultListByType
  }

defaultPersistIORef = unsafePerformIO $ newIORef filePersist

-- | Set the default persistence mechanism of all 'serializable' objects that have
-- @setPersist= const Nothing@. By default it is 'filePersist'
--
-- this statement must be the first one before any other TCache call
setDefaultPersist p = writeIORef defaultPersistIORef p

getDefaultPersist = unsafePerformIO $ readIORef defaultPersistIORef

getPersist x = unsafePerformIO $ case setPersist x of
     Nothing -> readIORef defaultPersistIORef
     Just p  -> return p
  `Exception.catch` (\ (e :: SomeException) -> error $
    "setPersist must depend on the type, not the value of the parameter for: "
      ++  show (typeOf x)
      ++ "error was:" ++ show e)

defaultReadByKey ::   String-> IO (Maybe B.ByteString)
defaultReadByKey k = handle handler $ do
  s <- readFileStrict k 
  return $ Just s -- `debug` ("read "++ filename)
 where
  handler :: IOError -> IO (Maybe B.ByteString)
  handler e
    | isAlreadyInUseError e = defaultReadByKey  k                         
    | isDoesNotExistError e = return Nothing
    | otherwise             = if "invalid" `isInfixOf` ioeGetErrorString e
        then error $ "defaultReadByKey: " ++ show e
          ++ " defPath and/or keyResource are not suitable for a file path:\n" ++ k ++ "\""
        else defaultReadByKey  k

defaultWrite :: String-> B.ByteString -> IO()
defaultWrite filename x= safeWrite filename  x
safeWrite filename str= handle  handler  $ B.writeFile filename str   -- !> ("write "++filename)
     where          
     handler e -- (e :: IOError)
       | isDoesNotExistError e = do
           createDirectoryIfMissing True $ take (1+(last $ elemIndices '/' filename)) filename -- maybe the path does not exist
           safeWrite filename str               
       | otherwise = if ("invalid" `isInfixOf` ioeGetErrorString e)
           then
             error  $ "defaultWriteResource: " ++ show e ++ " defPath and/or keyResource are not suitable for a file path: "++ filename
           else do
             hPutStrLn stderr $ "defaultWriteResource: " ++ show e ++ " in file: " ++ filename ++ " retrying"
             safeWrite filename str
              
defaultDelete :: String -> IO ()
defaultDelete filename = handle (handler filename) $ removeFile filename where
  handler :: String -> IOException -> IO ()
  handler file e
    | isDoesNotExistError e= return ()  --`debug` "isDoesNotExistError"
    | isAlreadyInUseError e= do
        hPutStrLn stderr $ "defaultDelResource: busy"  ++  " in file: " ++ filename ++ " retrying"
        -- threadDelay 100000   --`debug`"isAlreadyInUseError"
        defaultDelete filename  
    | otherwise = do
        hPutStrLn stderr $ "defaultDelResource: " ++ show e ++ " in file: " ++ filename ++ " retrying"
        -- threadDelay 100000     --`debug` ("otherwise " ++ show e)
        defaultDelete filename

defaultListByType :: forall t. (Typeable t) => FilePath -> Proxy t -> IO [Key]
defaultListByType dir _ = do
  files <- getDirectoryContents dir
  return . catMaybes . map (stripPrefix $ typeString ++ "-") $ files
 where
  typeString = show (typeOf (undefined :: t))

defReadResourceByKey :: forall t. (Indexable t,Serializable t,Typeable t) => Key -> IO (Maybe t)
defReadResourceByKey k = do
  let Persist f _ _ _ = getPersist (undefined :: t)
  f (filePath (Proxy :: Proxy t) k) >>= evaluate . fmap (deserialKey k)

defWriteResource :: forall t. (Indexable t,Serializable t,Typeable t)
  => t -> IO ()
defWriteResource s = do
  let Persist _ f _ _ = getPersist s
  f (filePath (Proxy :: Proxy t) $ key s) $ serialize s

defDelResource :: forall t. (Indexable t,Serializable t,Typeable t)
  => t -> IO ()
defDelResource s = do
  let Persist _ _ f _ = getPersist s
  f $ filePath (Proxy :: Proxy t) (key s)

defListResources :: forall t. (Serializable t,Indexable t,Typeable t) => Proxy t -> IO [Key]
defListResources p = do
  let Persist _ _ _ f = getPersist (undefined :: t)
  f (defPath (undefined :: t)) p

filePath :: forall t. (Indexable t,Typeable t) => Proxy t -> Key -> FilePath
filePath _ k = defPath (undefined :: t) ++ typeString ++ "-" ++ k where
  typeString = show $ typeOf (undefined :: t)

-- | Strict read from file, needed for default file persistence
readFileStrict f = openFile f ReadMode >>= \ h -> readIt h `finally` hClose h
  where
  readIt h= do
      s   <- hFileSize h
      let n= fromIntegral s
      str <- B.hGet h n
      return str
