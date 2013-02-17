{-# LANGUAGE BangPatterns, NamedFieldPuns#-}
module SafeHash(HashTable(), new, new_, new', new_', htInsert, htDelete, htLookup, fromList, toList, mapReduce) where

import Control.Concurrent.STM.TArray
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.STM
import Data.Array.MArray
import Data.Maybe
import Data.Word

minTableSize = 8 :: Word32
extensionSize = 10 :: Word32

data HashTable key val = HashTable {cmp :: !(key -> key -> Ordering), hash :: !(key -> Word32), table :: TVar (HT key val)}
data HT key val = HT {usage :: !Word32, eSize :: !Word32, buckets :: !(TArray Word32 (Tree key val))}
data Tree key val = Node key val (Tree key val) (Tree key val) | Leef

newOrd :: Ord key => (key -> Word32) -> IO (HashTable key val)
newOrd = new_ compare

new :: Eq key => (key -> Word32) -> IO (HashTable key val)
new = new_ eqCompare

new_ :: (key -> key -> Ordering) -> (key -> Word32) -> IO (HashTable key val)
new_ cmp hash = new_' cmp hash minTableSize

new' :: (Eq key) => (key -> Word32) -> Word32 -> IO (HashTable key val)
new' hash size = new_' (makeCompare (==)) hash size

new_' :: (key -> key -> Ordering) -> (key -> Word32) -> Word32 -> IO (HashTable key val)
new_' cmp hash size = newWithExtensionSize_' cmp hash size extensionSize


newWithExtensionSize_' :: (key -> key -> Ordering) -> (key -> Word32) -> Word32 -> Word32 -> IO (HashTable key val)
newWithExtensionSize_' cmp hash size eSize = do
     buckets <- atomically $ newArray (0, size-1) Leef
     let table = HT {usage = 0, eSize = eSize, buckets=buckets}
     ref <- newTVarIO table
     return (HashTable {cmp=cmp, hash=hash, table=ref})

htInsert :: HashTable key val -> key -> val -> IO ()
htInsert ht k v = atomically $ htInsert' ht k v

htInsert' :: HashTable key val -> key -> val -> STM ()
htInsert' (ht@HashTable{cmp, hash, table=ref}) k v = do
       iht@HT{usage, buckets} <- readTVar ref
       (0, size) <- getBounds buckets
       when (tooBig usage size) (rehash ht)
       (0, size') <- getBounds buckets
       let indx = (hash k) `mod` size'
       bucket <- readArray buckets indx
       writeArray buckets indx (tInsert cmp bucket k v)
       writeTVar ref (iht{usage=usage+1})

htDelete :: HashTable key val -> key -> IO ()
htDelete table k = atomically $ htDelete' table k

htDelete' :: HashTable key val -> key -> STM ()
htDelete' (ht@HashTable{cmp, hash, table=ref}) k = do
                                          HT{buckets} <- readTVar ref
                                          (0, size) <- getBounds buckets
                                          let indx = (hash k) `mod` size
                                          bucket <- readArray buckets indx
                                          writeArray buckets indx (tDelete cmp bucket k)
                

htLookup :: HashTable key val -> key -> IO (Maybe val)
htLookup ht k = atomically $ htLookup' ht k

htLookup' :: HashTable key val -> key -> STM (Maybe val)
htLookup' ht@HashTable{cmp, hash, table=ref} k = do
                                          HT{buckets} <- readTVar ref
                                          (0, size) <- getBounds buckets
                                          let indx = (hash k) `mod` size
                                          fmap (flip (tLookup cmp) k) (readArray buckets indx)

tooBig :: Word32 -> Word32 -> Bool
tooBig usage size = usage > 7 * size + 64

rehash :: HashTable key val -> STM ()
rehash ht@HashTable{cmp, hash, table=ref} = do
                                          HT{buckets, eSize} <- readTVar ref
                                          (0, size) <- getBounds buckets
                                          if size <= maxBound - eSize
                                          then do
                                               kvs <- fmap (concat . map tCollapse) (getElems buckets)
                                               buckets' <- newArray (0, size + eSize) Leef
                                               writeTVar ref (HT {usage = 0, eSize = eSize, buckets=buckets'})
                                               mapM_ (uncurry $ htInsert' ht) kvs
                                          else return ()

fromList :: (Eq key) => (key -> Word32) -> [(key, val)] -> IO (HashTable key val)
fromList = fromList' (==)

fromList' :: (key -> key -> Bool) -> (key -> Word32) -> [(key, val)] -> IO (HashTable key val)
fromList' cmp = fromListOrd' (makeCompare cmp)

fromListOrd :: (Ord key) => (key -> Word32) -> [(key, val)] -> IO (HashTable key val)
fromListOrd = fromListOrd' compare

fromListOrd' :: (key -> key -> Ordering) -> (key -> Word32) -> [(key, val)] -> IO (HashTable key val)
fromListOrd' cmp hash ls = do
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
                            fmap (concat . map (tCollapse . snd)) . getAssocs $ buckets
                            
mapReduce :: ((key, val) -> r) -> ([r] -> r) -> HashTable key val -> IO r
mapReduce m r table = atomically $ mapReduce' m r table
                                 
mapReduce' :: ((key, val) -> r) -> ([r] -> r) -> HashTable key val -> STM r
mapReduce' m r table = fmap (r . map m) (toList' table)

-- Sorted Binary Key Value Association Trees

instance Functor (Tree key) where
         fmap f Leef = Leef
         fmap f (Node k v t1 t2) = Node k (f v) (fmap f t1) (fmap f t2)

tInsert :: (key -> key -> Ordering) -> Tree key val -> key -> val -> Tree key val
tInsert cmp Leef k v = Node k v Leef Leef
tInsert cmp (Node k' v' t1 t2) k v = case cmp k' k of
                 LT -> Node k' v' (tInsert cmp t1 k v) t2
                 GT -> Node k' v' t1 (tInsert cmp t2 k v)
                 otherwise -> Node k' v t1 t2

tDelete :: (key -> key -> Ordering) -> Tree key val -> key -> Tree key val
tDelete cmp Leef k = Leef
tDelete cmp (Node k' v t1 t2) k = case cmp k' k of
           LT -> Node k' v (tDelete cmp t1 k) t2
           GT -> Node k' v t1 (tDelete cmp t2 k)
           otherwise -> undefined

tLookup :: (key -> key -> Ordering) -> Tree key val -> key -> Maybe val
tLookup cmp Leef k = Nothing
tLookup cmp (Node k' v t1 t2) k = case cmp k' k of
           LT -> tLookup cmp t1 k
           GT -> tLookup cmp t2 k
           EQ -> Just v

tCollapse :: Tree key val -> [(key, val)]
tCollapse Leef = []
tCollapse (Node k v t1 t2) = (k,v):((tCollapse t1) ++ (tCollapse t2))

makeCompare :: (a -> a -> Bool) -> a -> a -> Ordering
makeCompare eq a b = if a `eq` b
                     then EQ
                     else GT

eqCompare :: Eq a => a -> a -> Ordering
eqCompare = makeCompare (==)
