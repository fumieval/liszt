{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where
import Database.Liszt
import Database.Liszt.Network

import Control.Monad
import Data.Function (fix)
import qualified Data.ByteString.Char8 as B
import System.Environment
import System.IO
import System.Console.GetOpt
import System.Exit

parseHostPort :: String -> (String -> Int -> r) -> r
parseHostPort str k = case break (==':') str of
  (host, ':' : port) -> k host (read port)
  (host, _) -> k host 1886

data Options = Options
  { host :: String
  , timeout :: Double
  , ranges :: [(Offset, Offset)]
  , beginning :: Maybe Offset
  }

readOffset :: String -> Offset
readOffset ('_' : n) = FromEnd (read n)
readOffset n = SeqNo (read n)

options :: [OptDescr (Options -> Options)]
options = [Option "h" ["host"] (ReqArg (\str o -> o { host = str }) "HOST:PORT") "stream input"
  , Option "r" ["range"] (ReqArg (\str o -> o { ranges = case break (==':') str of
      (begin, ':' : end) -> (readOffset begin, readOffset end) : ranges o
      _ -> (readOffset str, readOffset str) : ranges o
      }) "FROM:TO") "ranges"
  , Option "b" ["begin"] (ReqArg (\str o -> o { beginning = Just $! readOffset str }) "pos") "get all the contents from this position"
  , Option "t" ["timeout"] (ReqArg (\str o -> o { timeout = read str }) "SECONDS") "Timeout"
  ]

defaultOptions :: Options
defaultOptions = Options
  { host = "localhost"
  , timeout = 1
  , ranges = []
  , beginning = Nothing
  }

printBS :: (a, B.ByteString, B.ByteString) -> IO ()
printBS (_, tag, _) = do
  B.hPutStr stdout tag
  hFlush stdout

main :: IO ()
main = getOpt Permute options <$> getArgs >>= \case
  (fs, path : name : _, []) -> do
    let o = foldl (flip id) defaultOptions fs
    parseHostPort (host o) withConnection (B.pack path) $ \conn -> do
      let name' = B.pack name
      let timeout' = floor $ timeout o * 1000000
      let req i j = Request name' timeout' i j
      forM_ (reverse $ ranges o) $ \(i, j) -> do
        bss <- fetch conn $ req i j
        mapM_ printBS bss
      forM_ (beginning o) $ \start -> do
        bss0 <- fetch conn $ req start start
        mapM_ printBS bss0
        unless (null bss0) $ do
          let (start', _, _) = last bss0
          flip fix (start' + 1) $ \self i -> do
            bss <- fetch conn $ req (SeqNo i) (SeqNo i)
            mapM_ printBS bss
            unless (null bss) $ self $ let (j, _, _) = last bss in j + 1

  (_, _, es) -> do
    name <- getProgName
    die $ unlines es ++ usageInfo name options
