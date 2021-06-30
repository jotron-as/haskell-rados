# haskell-rados
Experimental Rados Bindings for haskell. They are very much inspired by the already existsing
haskell bindings, but
- Are in MTL style
- Use [inline-c][inline-c] for actual binding generation.

The reason it is in MTL style is because, if you are using cluster storage, then you are probably
very into microservices. There is a haskell design pattern where each microservice is interfaced
with via a monad transformer. This allows for stubbing goodness when testing and abstracts away
a lot of awful error handeling messes.

## Example Library Usage
Here is an application that takes a picture using ffmpeg and stores it in a new ceph pool called
`temp-cam`. The key is the time stamp of when the picture was taken.

```haskell
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           System.IO.Rados
import           Shelly
import           Control.Monad.Fix       (fix)
import           Control.Monad.Except
import           Data.Time
import qualified Data.Text               as T
import qualified Data.ByteString         as BS
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy    as BL

type Device = T.Text

takePicture_ :: Device -> FilePath -> Sh ()
takePicture_ dev out = run_ "ffmpeg" $ ["-y", "-f", "video4linux2", "-i", dev
                                       , "-ss", "0:0:1", "-frames", "1"
                                       ] ++ [T.pack out]

takePicture d = shelly . silently . takePicture_ d

conf = CephConfig { configFile = "/etc/ceph/ceph.conf"
                  , userName   = "admin"
                  , timeLimit  = 1000000 -- 1 sec
                  }

time2key = BS.toStrict . toLazyByteString . string8 . show

downloadPics :: IO (Either RadosError ())
downloadPics = withConfig conf $ inNewDefaultPool "temp-cam" $ do
  fix $ \loop -> do
    liftIO $ takePicture "/dev/video0" "/tmp/out.jpg"
    key <- liftIO $ time2key <$> getZonedTime
    (h:t) <- liftIO $ BL.toChunks <$> BL.readFile "/tmp/out.jpg"
    writeFull key h
    mapM (append key) t
    loop
  return ()


main :: IO ()
main = downloadPics >>= either print (const $ putStrLn "Done")
```

## Docs
haddock docs are to be found [here][docs].

[inline-c]: https://hackage.haskell.org/package/inline-c
[docs]: https://www.hobson.space/docs/rados/
