-- -*- haskell-mode -*-

{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.Framing where

import Network.AMQP.FramingData
import Network.AMQP.FramingTypes
import Network.AMQP.Types

import Data.Binary ( Binary(..), Get, Put )

$(genContentHeaderProperties domainMap classes)

$(genClassIDFuns classes)
getClassIDOf :: (Num a) => ContentHeaderProperties -> a

$(genMethodPayload domainMap classes)

$(genGetContentHeaderProperties classes)
getContentHeaderProperties :: (Num a) => a -> Get ContentHeaderProperties

$(genPutContentHeaderProperties classes)
putContentHeaderProperties :: ContentHeaderProperties -> Put

$(genMethodPayloadBinaryInstance domainMap classes)
