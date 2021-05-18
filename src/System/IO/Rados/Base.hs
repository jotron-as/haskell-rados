{-# LANGUAGE GeneralisedNewtypeDeriving, FlexibleContexts, KindSignatures #-}
{-# LANGUAGE QuasiQuotes, TemplateHaskell, ScopedTypeVariables #-}

module System.IO.Rados
( -- * Types
  ConnectionT(..)
, Connection(..)
, CephConfig(..)
, PoolT(..)
, Pool(..)
, CrushRule
, Key
  -- * Connection Functions
, withConfig
-- * Classes
, MonadConnection
, MonadPool
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

type Connection a = ConnectionT IO a
type CrushRule = Word8

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

class (MonadIO m, MonadError RadosError m) => MonadConnection m where
  cleanUp :: m () -- ^ Cleans up 'RadosT'
  radosConfReadFile :: FilePath -> m Int
  radosConnectToCluster :: m Int
  inPool :: String -> Pool a -> m a
  inNewPool :: String -> CrushRule -> Pool a -> m a
  inNewDefaultPool :: String -> Pool a -> m a
  removePool       :: String -> m ()

instance (MonadIO m) => MonadConnection (ConnectionT m) where
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
    res <- liftIO $ runPoolT ( do { ret <- pool `catchError` cleanUpPool'
                                  ; cleanUpPool
                                  ; return ret
                                  }) ioctx
    either throwError return res


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

cleanUp' :: (MonadConnection m) => RadosError -> m a
cleanUp' e = cleanUp >> throwError e

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

class (Monad p) => MonadPool p where
  cleanUpPool :: p ()
  writeFull   :: Key -> B.ByteString -> p ()
  append      :: Key -> B.ByteString -> p ()
  writeAt     :: Key -> Int -> B.ByteString -> p ()
  readAt      :: Key -> Int -> Int -> p B.ByteString
  comp        :: Key -> B.ByteString -> p Bool
  comp key dat = compAt key 0 dat
  compAt      :: Key -> Int -> B.ByteString -> p Bool
  remove      :: Key -> p ()

instance (MonadIO m) => MonadIO (PoolT m) where
  liftIO io = PoolT $ ExceptT $ ReaderT $ \r -> do
    res <- liftIO $ try io
    case res of
      Left e  -> return $ Left $ User $ displayException (e :: IOException)
      Right x -> return $ Right $ x

instance (MonadIO m) => MonadPool (PoolT m) where
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
