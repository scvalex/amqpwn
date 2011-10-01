{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.Framing (
        ContentHeaderProperties(..), MethodPayload(..),
        getClassIdOf, getContentHeaderProperties, putContentHeaderProperties
    ) where

import Network.AMQP.FramingData
import Network.AMQP.FramingTypes
import Network.AMQP.Types.Internal

import Data.Binary ( Binary(..), Get, Put )

$(genContentHeaderProperties domainMap classes)

$(genClassIdFuns classes)
getClassIdOf :: (Num a) => ContentHeaderProperties -> a

$(genMethodPayload domainMap classes)

$(genGetContentHeaderProperties classes)
getContentHeaderProperties :: (Num a) => a -> Get ContentHeaderProperties

$(genPutContentHeaderProperties classes)
putContentHeaderProperties :: ContentHeaderProperties -> Put

$(genMethodPayloadBinaryInstance domainMap classes)
