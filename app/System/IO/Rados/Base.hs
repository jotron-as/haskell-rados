{-# LANGUAGE GeneralisedNewtypeDeriving, FlexibleContexts, KindSignatures #-}

module System.IO.Rados.Base
( ConnectionT(..)
, Connection(..)
, CephConfig(..)
, PoolT(..)
, Pool(..)
, withConfig
, runConnection
) where

import Control.Monad.Identity
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Trans
import Control.Exception
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import System.IO

import qualified Data.ByteString as B

import System.IO.Rados.Error

newtype ConnectionT (m :: * -> *) a = ConnectionT (ExceptT RadosError (ReaderT CephConfig m) a)
  deriving (Functor,Applicative,Monad,MonadError RadosError, MonadReader CephConfig)

type Connection a = ConnectionT IO a

data CephConfig = CephConfig
  { configFile :: FilePath
  , user       :: FilePath
  }

instance (MonadIO m) => MonadIO (ConnectionT m) where
  liftIO io = ConnectionT $ ExceptT $ ReaderT $ \r -> do
    res <- liftIO $ try io
    case res of
      Left e  -> return $ Left $ User $ displayException (e :: IOException)
      Right x -> return $ Right $ x


tryS :: (MonadIO m, MonadError RadosError m) => String -> IO CInt -> m Int
tryS function action = do
  n <- liftIO action
  if n < 0 then do
    let errno = (-n)
    strerror <- liftIO $ peekCString =<< c_strerror (Errno errno)
    throwError $ makeError (fromIntegral errno) function strerror
  else return $ fromIntegral n

runConnection :: (Monad m) => CephConfig -> ConnectionT m a -> m (Either RadosError a)
runConnection conf (ConnectionT c) = runReaderT (runExceptT c) conf

withConfig :: CephConfig -> Connection a -> IO (Either RadosError a)
withConfig = runConnection

newtype PoolT (m :: * -> *) a = PoolT (m a)
  deriving (Functor,Applicative,Monad)

type Pool a = PoolT IO a

class (Monad m) => MonadConnection m where
  runTransaction :: (MonadPool p) => p a -> m a

instance (MonadIO m) => MonadConnection (ConnectionT m) where
  runTransaction pool = undefined

class (Monad p) => MonadPool p where
  newPool :: String -> p ()

instance (MonadIO m) => MonadPool (PoolT m) where
  newPool name = undefined
