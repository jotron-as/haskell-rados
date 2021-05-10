{-# LANGUAGE OverloadedStrings #-}

module Main where

import System.IO.Rados.Base

conf = CephConfig { configFile = "/etc/ceph/ceph.conf"
                  , userName   = "admin"
                  }

main :: IO ()
main = do
  putStrLn "Hello, Haskell!"
  result <- withConfig conf $ inPool "mypool" $ do
    writeFull "key" "James is my name"
    readAt "key" 16 0
  either print print result
