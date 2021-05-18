{-# LANGUAGE RecordWildCards    #-}

module System.IO.Rados.Error
( RadosError(..)
, makeError
, c_strerror
) where

import Foreign
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import Control.Exception

instance Exception RadosError

data RadosError = Unknown  { errno     :: Int    -- ^ Error number (positive)
                           , cFunction :: String -- ^ The underlying C function
                           , strerror  :: String -- ^ The \"nice\" error message.
                           }
                -- | Usually returned if a file does not exist
                | NoEntity { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                -- | Returned if a file already exists, and should not.
                | Exists   { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                -- | Returned in the event of a failed atomic transaction
                | Canceled { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                -- | A value was out of range, returned when reading or writing
                -- from/to invalid regions.
                | Range    { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                | User     { message :: String }
    deriving (Eq, Ord)

instance Show RadosError where
    show Unknown{..} = "rados: unknown rados error in '" ++
        cFunction ++ "', errno " ++ show errno ++ ": '" ++ strerror ++ "'"
    show NoEntity{..} = cFunction ++ ": ENOENT: '" ++ strerror ++ "'"
    show Exists{..} = cFunction ++ ": EEXIST: '" ++ strerror ++ "'"
    show Canceled{..} = cFunction ++ ": ECANCELED: '" ++ strerror ++ "'"
    show Range{..} = cFunction ++ ": ERANGE: '" ++ strerror ++ "'"
    show User{..} = "Rados user error: '" ++ message ++ "'"

makeError :: Int -> String -> String -> RadosError
makeError 125 fun str = Canceled 125 fun str
makeError 2 fun str   = NoEntity 2 fun str
makeError 17 fun str  = Exists 17 fun str
makeError 34 fun str  = Range 34 fun str
makeError n fun str   = Unknown n fun str

foreign import ccall safe "string.h strerror"
    c_strerror :: Errno -> IO (Ptr CChar)
