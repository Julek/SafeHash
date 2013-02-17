{-# LANGUAGE BangPatterns, NamedFieldPuns#-}
module SafeHash where

import Control.Concurrent.STM.TArray
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.STM
import Data.Array.IO
import Data.Array.MArray
import Data.IORef
import Data.Maybe
import Data.Word
import GHC.Conc.Sync

minTableSize = 8 :: Word32
extensionSize = 10 :: Word32

data HashTable key val = HashTable {cmp :: !(key -> key -> Bool), hash :: !(key -> Word32), table :: TVar (HT key val)}
data HT key val = HT {usage :: !Word32, buckets :: !(TArray Word32 [(key, val)])}

new :: Eq key => (key -> Word32) -> IO (HashTable key val)
new = new_ (==)

new_ :: (key -> key -> Bool) -> (key -> Word32) -> IO (HashTable key val)
new_ cmp hash = new_' cmp hash minTableSize

new' :: (Eq key) => (key -> Word32) -> Word32 -> IO (HashTable key val)
new' hash size = new_' (==) hash size

new_' :: (key -> key -> Bool) -> (key -> Word32) -> Word32 -> IO (HashTable key val)
new_' cmp hash size = do
     buckets <- atomically $ newArray (0, size-1) []
     let table = HT {usage = 0, buckets=buckets}
     ref <- newTVarIO table
     return (HashTable {cmp=cmp, hash=hash, table=ref})

htInsert :: HashTable key val -> key -> val -> IO ()
htInsert ht k v = atomically $ htInsert' ht k v

htInsert' :: HashTable key val -> key -> val -> STM ()
htInsert' (ht@HashTable{hash, table=ref}) k v = do
       iht@HT{usage, buckets} <- readTVar ref
       (0, size) <- getBounds $ buckets
       when (tooBig usage size) (rehash ht)
       (0, size') <- getBounds $ buckets
       let indx = (hash k) `mod` size'
       bucket <- readArray buckets indx
       writeArray buckets indx ((k, v):bucket)
       writeTVar ref (iht{usage=usage+1})

htLookup :: HashTable key val -> key -> IO (Maybe val)
htLookup ht k = atomically $ htLookup' ht k

htLookup' :: HashTable key val -> key -> STM (Maybe val)
htLookup' ht@HashTable{cmp, hash, table=ref} k = do
                                          HT{buckets} <- readTVar ref
                                          (0, size) <- getBounds $ buckets
                                          let indx = (hash k) `mod` size
                                          fmap (fmap snd . listToMaybe . filter (cmp k . fst)) (readArray buckets indx)

tooBig :: Word32 -> Word32 -> Bool
tooBig usage size = usage > 7 * size + 64

rehash :: HashTable key val -> STM ()
rehash ht@HashTable{cmp, hash, table=ref} = do
                                          HT{buckets=buckets} <- readTVar ref
                                          (0, size) <- getBounds $ buckets
                                          if size <= maxBound - extensionSize
                                          then do
                                               kvs <- fmap concat (getElems buckets)
                                               buckets' <- newArray (0, size + extensionSize) []
                                               writeTVar ref (HT {usage = 0, buckets=buckets'})
                                               mapM_ (uncurry $ htInsert' ht) kvs
                                          else return ()

fromList :: (Eq key) => (key -> Word32) -> [(key, val)] -> IO (HashTable key val)
fromList hash ls = fromList' (==) hash ls

fromList' :: (key -> key -> Bool) -> (key -> Word32) -> [(key, val)] -> IO (HashTable key val)
fromList' cmp hash ls = do
          let sz' = fromIntegral $ ((length ls) - 64) `div` 7
              sz = if sz' < minTableSize
                       then sz'
                       else minTableSize
          ret <- new_' cmp hash sz'
          mapM_ (uncurry $ htInsert ret) ls
          return ret

toList :: HashTable key val -> IO [(key, val)]
toList = atomically . toList'

toList' :: HashTable key val -> STM [(key, val)]
toList' HashTable{table=ref} = do
                            HT{buckets=buckets} <- readTVar ref
                            fmap (concat . map snd) . getAssocs $ buckets
                            
mapReduce :: ((key, val) -> r) -> ([r] -> r) -> HashTable key val -> IO r
mapReduce m r table = atomically $ fmap (r . map m) (toList' table)
                                 
                                 
