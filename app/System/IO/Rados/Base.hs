{-# LANGUAGE GeneralisedNewtypeDeriving, FlexibleContexts, KindSignatures #-}
{-# LANGUAGE QuasiQuotes, TemplateHaskell, ScopedTypeVariables #-}

module System.IO.Rados.Base
( ConnectionT(..)
, Connection(..)
, CephConfig(..)
, PoolT(..)
, Pool(..)
, withConfig
, writeFull
, readAt
, inPool
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

import qualified Language.C.Inline as C
import qualified Data.ByteString as B

import System.IO.Rados.Error

C.context (C.baseCtx <> C.bsCtx)

C.include "<rados/librados.h>"
C.include "<string.h>"

newtype ConnectionT (m :: * -> *) a = ConnectionT (ExceptT RadosError (ReaderT CephState m) a)
  deriving (Functor,Applicative,Monad,MonadError RadosError, MonadReader CephState)

type Connection a = ConnectionT IO a

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
  cleanUp :: m ()
  radosConfReadFile :: FilePath -> m Int
  radosConnectToCluster :: m Int
  inPool :: String -> Pool a -> m a

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
    res <- liftIO $ runPoolT pool ioctx
    either throwError return res


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

class (Monad p) => MonadPool p where
  writeFull :: B.ByteString -> B.ByteString -> p ()
  readAt    :: B.ByteString -> Int -> Int -> p B.ByteString

instance (MonadIO m) => MonadIO (PoolT m) where
  liftIO io = PoolT $ ExceptT $ ReaderT $ \r -> do
    res <- liftIO $ try io
    case res of
      Left e  -> return $ Left $ User $ displayException (e :: IOException)
      Right x -> return $ Right $ x

instance (MonadIO m) => MonadPool (PoolT m) where
  writeFull key dat = void $ asks getCtx >>= \ctx -> tryS "rados_write_full" 
    [C.exp| int { rados_write_full($(void* ctx), $bs-cstr:key, $bs-cstr:dat, $bs-len:dat) } |]

  readAt key len offset = do
    ctx <- asks getCtx
    let cLen = fromIntegral len
        cOff = fromIntegral offset
    str <- liftIO $ alloca $ \(out :: Ptr CString) -> do
      err <- [C.exp| int { rados_read($(void* ctx), $bs-cstr:key, *$(char** out), $(int cLen), $(int cOff)) } |]
      if err < 0 then do
        strerror <- liftIO $ peekCString =<< c_strerror (Errno $ fromIntegral err)
        throwIO $ makeError (fromIntegral err) "rados_ioctx_create" strerror
      else peek out
    res <- liftIO $ B.packCStringLen (str, len)
    -- liftIO $ free str
    return res

