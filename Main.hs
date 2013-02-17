import Data.Array.IO
import Data.Char
import Data.Int
import Data.List
import Data.Maybe

import SafeHash

main :: IO ()
main = do
     table <- new' fromIntegral 28
     mapM_ (\k -> htInsert table k (chr k)) [0..255]
     test <- toList table
     print ((sort . map fst $ test) == [0..255])
     
