{-# LANGUAGE GeneralisedNewtypeDeriving, FlexibleContexts, KindSignatures #-}
{-# LANGUAGE QuasiQuotes, TemplateHaskell, ScopedTypeVariables, FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-
Description: Monadic Bindings to librados

This is a experimental binding to librados. It uses the inline-c library making it very concise.
At the moment, the way pools interact with connections is clunkly, but hopfully, we will be
able to get it into the full MTL style. This is so that you can easily follow the design pattern
where components of your software are written as MTL style monad transformers and stacked to
allow for easy interactions between componenets.
-}
module System.IO.Rados
( -- * Types
  ConnectionT(..)
, Connection(..)
, CephConfig(..)
, PoolT(..)
, Pool(..)
, CrushRule
, Key
, RadosError
  -- * Connection Functions
, withConfig
, capture
-- * Connection Monad
, MonadConnection
, inPool
, inNewPool
, inNewDefaultPool
, removePool
  -- * Pool Monad
, MonadPool
, writeFull
, append
, writeAt
, readAt
, comp
, compAt
, remove
) where

import Control.Monad
import Control.Monad.Identity
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Trans
import Control.Exception
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc
import System.IO
import Data.Maybe
import Data.Word

import qualified Language.C.Inline as C
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B

import System.IO.Rados.Error

C.context (C.baseCtx <> C.bsCtx)

C.include "<rados/librados.h>"
C.include "<string.h>"

newtype ConnectionT (m :: * -> *) a = ConnectionT (ExceptT RadosError (ReaderT CephState m) a)
  deriving (Functor,Applicative,Monad,MonadError RadosError, MonadReader CephState)

instance (Monad m) => MonadFail (ConnectionT m) where
  fail = throwError . User

instance MonadTrans ConnectionT where
  lift x = ConnectionT $ ExceptT $ ReaderT $ \_ -> Right <$> x

liftEitherT x = ConnectionT $ ExceptT $ ReaderT $ \_ -> x

type Connection a = ConnectionT IO a
type CrushRule = Word8

-- | Helps you glue exceptions together when using PoolT or ConnectionT
capture :: (Show e, MonadError RadosError m) => Either e a -> m a
capture (Left err) = throw $ User $ show err
capture (Right x)  = return x

data CephConfig = CephConfig
  { configFile  :: FilePath
  , userName    :: String
  }

type RadosT = Ptr ()
newtype CephState = CephState { getPtr :: RadosT }

tryS :: (MonadError RadosError m, MonadIO m) => String -> IO CInt -> m Int
tryS function action = do
  n <- liftIO action
  if n < 0 then do
    let errno = (-n)
    strerror <- liftIO $ peekCString =<< c_strerror (Errno errno)
    throwError $ makeError (fromIntegral errno) function strerror
  else return $ fromIntegral n


instance (MonadIO m) => MonadIO (ConnectionT m) where
  liftIO io = ConnectionT $ ExceptT $ ReaderT $ \r -> do
    res <- liftIO $ try io
    case res of
      Left e  -> return $ Left $ User $ displayException (e :: IOException)
      Right x -> return $ Right $ x

-- | Generic Monad for communicating with ceph
class (MonadIO m, MonadError RadosError m, MonadIO n) => MonadConnection m n | m -> n where
  cleanUp :: m ()
  radosConfReadFile :: FilePath -> m Int
  radosConnectToCluster :: m Int
  -- | Inside named pool, run a pool transaction
  inPool :: String -> PoolT n a -> m a
  -- | Inside a new pool (with name and crush rule given), run transaction
  inNewPool :: String -> CrushRule -> PoolT n a -> m a
  -- | Inside new pool (default crush rule), run transaction
  inNewDefaultPool :: String -> PoolT n a -> m a
  -- | delete pool (requires ceph to be configured to allow pool deletion)
  removePool :: String -> m ()

instance (MonadIO m) => MonadConnection (ConnectionT m) m where
  cleanUp = do
    CephState ptr <- ask
    liftIO $ [C.exp| void { rados_shutdown($(void* ptr)) } |]

  radosConfReadFile file = do
    CephState ptr <- ask
    tryS "rados_conf_read_file" $ withCString file $ \cFile  ->
      [C.exp| int { rados_conf_read_file($(void* ptr), $(char* cFile)) } |]

  radosConnectToCluster = asks getPtr >>= \ptr -> tryS "rados_connect" [C.exp| int { rados_connect($(void* ptr)) } |]

  inPool poolname pool = asks getPtr >>= \ptr -> do
    ioctx <- liftIO $ withCString poolname $ \pn -> do
      alloca $ \ioctx_pp -> do
        err <- [C.exp| int { rados_ioctx_create($(void* ptr), $(char* pn), $(void** ioctx_pp)) } |]
        if err < 0 then do
          strerror <- liftIO $ peekCString =<< c_strerror (Errno $ fromIntegral err)
          throwIO $ makeError (fromIntegral err) "rados_ioctx_create" strerror
        else PoolCfg <$> peek ioctx_pp
    liftEitherT $ runPoolT ( do { ret <- pool `catchError` cleanUpPool'
                         ; cleanUpPool
                         ; return ret
                         }) ioctx

  inNewPool name crushRule pool = do
    ptr <- asks getPtr
    tryS "rados_pool_create_with_crush_rule" $ withCString name $ \pool_name ->
      [C.exp| int { rados_pool_create_with_crush_rule($(void* ptr), $(char* pool_name), $(uint8_t crushRule)) } |]
    inPool name pool

  inNewDefaultPool n = inNewPool n 0

  removePool s = void $ do
    ptr <- asks getPtr
    tryS "rados_pool_delete" $ withCString s $ \pool_name ->
      [C.exp| int { rados_pool_delete($(void* ptr), $(char* pool_name)) } |]


runConnection :: (MonadIO m) => ConnectionT m a -> CephState -> m (Either RadosError a)
runConnection (ConnectionT c) conf = runReaderT (runExceptT c) conf

cleanUp' :: (MonadConnection m n) => RadosError -> m a
cleanUp' e = cleanUp >> throwError e

-- | With ceph config data, open a ceph connection.
withConfig :: (MonadIO m) => CephConfig -> ConnectionT m a -> m (Either RadosError a)
withConfig conf c = do
  ptr <- liftIO $ try $ withCString (userName conf) $ \clName ->
      alloca $ \rados_t_p_p -> do
        ret <- [C.block| int {
          return rados_create($(void** rados_t_p_p), $(char* clName));
        } |]
        if ret < 0 then do
          strerror <- liftIO $ peekCString =<< c_strerror (Errno $ fromIntegral ret)
          throwIO $ makeError (fromIntegral ret) "rados_create2" strerror
        else CephState <$> peek rados_t_p_p
  case ptr of
    Left e     -> return $ Left e
    Right ptr' -> flip runConnection ptr' $ do
      (radosConfReadFile $ configFile conf)
      radosConnectToCluster `catchError` cleanUp'
      ret <- c `catchError` cleanUp'
      cleanUp
      return ret
        
type RadosIoctxT = Ptr ()
newtype PoolConfig = PoolCfg { getCtx :: RadosIoctxT }

newtype PoolT (m :: * -> *) a = PoolT (ExceptT RadosError (ReaderT PoolConfig m) a)
  deriving (Functor,Applicative,Monad,MonadReader PoolConfig, MonadError RadosError)

runPoolT :: (MonadIO m) => PoolT m a -> PoolConfig -> m (Either RadosError a)
runPoolT (PoolT xm) cfg = flip runReaderT cfg $ runExceptT xm

type Pool a = PoolT IO a
type Key    = B.ByteString

class (Monad p, Monad q) => MonadPool p q | p -> q where
  cleanUpPool :: p ()
  -- | Write bytestring object with key
  writeFull   :: Key -> B.ByteString -> p ()
  -- | append with bytestring
  append      :: Key -> B.ByteString -> p ()
  -- | write at a certain offset
  writeAt     :: Key -> Int -> B.ByteString -> p ()
  -- | get an object out of ceph
  readAt      :: Key 
              -> Int -- ^ Length to read
              -> Int -- ^ Offset to read from
              -> p B.ByteString
  -- | Compare object with bytestring
  comp        :: Key -> B.ByteString -> p Bool
  comp key dat = compAt key 0 dat
  -- | compare object (from certain offset) with bytestring
  compAt      :: Key -> Int -> B.ByteString -> p Bool
  -- | Delete object
  remove      :: Key -> p ()

instance (MonadIO m) => MonadIO (PoolT m) where
  liftIO io = PoolT $ ExceptT $ ReaderT $ \r -> do
    res <- liftIO $ try io
    case res of
      Left e  -> return $ Left $ User $ displayException (e :: IOException)
      Right x -> return $ Right $ x

instance (Monad m) => MonadFail (PoolT m) where
  fail = throwError . User . show

instance MonadTrans PoolT where
  lift x = PoolT $ ExceptT $ ReaderT $ \_ -> Right <$> x

instance (MonadIO m) => MonadPool (PoolT m) m where
  writeFull key dat = void $ asks getCtx >>= \ctx -> tryS "rados_write_full" 
    [C.exp| int { rados_write_full($(void* ctx), $bs-cstr:key, $bs-cstr:dat, $bs-len:dat) } |]

  append key dat = void $ asks getCtx >>= \ctx -> tryS "rados_append" 
    [C.exp| int { rados_append($(void* ctx), $bs-cstr:key, $bs-cstr:dat, $bs-len:dat) } |]

  readAt key len offset = do
    ctx <- asks getCtx
    let cLen = fromIntegral len
        cOff = fromIntegral offset
    out <- liftIO $ mallocBytes (fromIntegral len)
    tryS "rados_read" [C.exp| int { rados_read($(void* ctx), $bs-cstr:key, $(char* out), $(int cLen), $(int cOff)) } |]
    liftIO $ B.unsafePackMallocCStringLen (out, len)

  remove key = void $ asks getCtx >>= \ctx -> tryS "rados_remove"
    [C.exp| int { rados_remove($(void* ctx), $bs-cstr:key) } |]

  cleanUpPool = void $ asks getCtx >>= \ctx ->
    liftIO $ [C.exp| void { rados_ioctx_destroy($(void* ctx)) } |]

  writeAt key offset dat = void $ asks getCtx >>= \ctx -> let off = fromIntegral offset in tryS "rados_write"
    [C.exp| int { rados_write($(void* ctx), $bs-cstr:key, $bs-cstr:dat, $bs-len:dat, $(int off)) } |]

  compAt key off dat = do
    ctx <- asks getCtx
    let offset = fromIntegral off
    res <- liftIO $ [C.exp| int { rados_cmpext($(void* ctx), $bs-cstr:key, $bs-cstr:dat, $bs-len:dat, $(int offset)) } |]
    return $ res == 0

cleanUpPool' e = cleanUpPool >> throwError e
