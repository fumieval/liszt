{-# LANGUAGE OverloadedStrings #-}
import Gauge.Main
import Control.Monad
import Database.Liszt

main = withLiszt "bench.liszt" $ \h -> do
    defaultMain
        [ bench "insert" $ nfIO $ commit h $ do
          forM_ [1..10] $ \i -> insert "test" (i :: Int) ]
