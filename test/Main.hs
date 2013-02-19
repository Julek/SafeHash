import Data.Array.IO
import Data.Char
import Data.Int
import Data.List
import Data.Maybe
import Data.SafeHash
import System.Random.Shuffle

main :: IO ()
main = do
     ls <- shuffleM [0..255] :: IO [Int]
     table <- fromListOrd fromIntegral . map (\k -> (k, chr k)) $ ls
     test <- toList table
     print test
