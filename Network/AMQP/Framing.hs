-- -*- haskell-mode -*-

{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.Framing where

import Network.AMQP.FramingData
import Network.AMQP.FramingTypes
import Network.AMQP.Types

import Data.Binary.Get ( Get, getWord8 )
import Data.Binary.Put ( Put, putWord8 )
import Data.Bits
import Data.Word ( Word8 )

-- * Bits need special handling because AMQP requires contiguous bits
-- to be packed into a Word8

-- | Packs up to 8 bits into a Word8
putBits :: [Bit] -> Put
putBits = putWord8 . putBits' 0
    where
      putBits' _ [] = 0
      putBits' offset (x:xs) =
          (shiftL (toInt x) offset) .|. (putBits' (offset+1) xs)
              where
                toInt True = 1
                toInt False = 0

getBits :: Integer -> Get [Bit]
getBits num = getWord8 >>= \x -> return $ getBits' num 0 x
    where
      getBits' :: Integer -> Integer -> Word8 -> [Bit]
      getBits' 0 _ _ = []
      getBits' n offset x =
          ((x .&. (2 ^ offset)) /= 0) : (getBits' (n-1) (offset+1) x)

$(genContentHeaderProperties domainMap classes)

$(genClassIDFuns classes)

$(genMethodPayload domainMap classes)

$(genGetContentHeaderProperties classes)

$(genPutContentHeaderProperties classes)
