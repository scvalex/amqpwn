-- -*- haskell-mode -*-

{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.Framing where

import Network.AMQP.FramingData
import Network.AMQP.FramingTypes
import Network.AMQP.Types

import Data.Binary.Get ( Get, getWord8 )
import Data.Binary.Put ( Put, putWord8 )
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

$(genContentHeaderProperties domainMap classes)

$(genClassIDFuns classes)

$(genMethodPayload domainMap classes)

$(genGetContentHeaderProperties classes)

$(genPutContentHeaderProperties classes)
