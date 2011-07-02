-- -*- haskell-mode -*-

{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.Framming where

import Network.AMQP.FrammingData
import Network.AMQP.FrammingTypes
import Network.AMQP.Types

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.Maybe

-- * Bits need special handling because AMQP requires contiguous bits
-- to be packed into a Word8

-- | Packs up to 8 bits into a Word8
putBits :: [Bit] -> Put
putBits xs = putWord8 $ putBits' 0 xs
putBits' _ [] = 0
putBits' offset (x:xs) =
    (shiftL (toInt x) offset) .|. (putBits' (offset+1) xs)
        where
          toInt True = 1
          toInt False = 0

getBits num = getWord8 >>= \x -> return $ getBits' num 0 x
getBits' 0 offset _= []
getBits' num offset x =
    ((x .&. (2 ^ offset)) /= 0) : (getBits' (num-1) (offset+1) x)

-- | Packs up to 15 Bits into a Word16 (=Property Flags)
putPropBits :: [Bit] -> Put
putPropBits xs = putWord16be $ (putPropBits' 0 xs)
putPropBits' _ [] = 0
putPropBits' offset (x:xs) =
    (shiftL (toInt x) (15-offset)) .|. (putPropBits' (offset+1) xs)
        where
          toInt True = 1
          toInt False = 0

getPropBits num = getWord16be >>= \x -> return $ getPropBits' num 0  x
getPropBits' 0 offset _= []
getPropBits' num offset x =
    ((x .&. (2^(15-offset))) /= 0) : (getPropBits' (num-1) (offset+1) x)

condGet False = return Nothing
condGet True = get >>= \x -> return $ Just x

condPut (Just x) = put x
condPut _ = return ()

$(genClassIDs classes)
