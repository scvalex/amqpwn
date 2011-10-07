{-# LANGUAGE TemplateHaskell #-}

module Network.Messaging.AMQP.Framing (
        ContentHeaderProperties(..), MethodPayload(..),
        getClassIdOf, getContentHeaderProperties, putContentHeaderProperties
    ) where

import Network.Messaging.AMQP.FramingData
import Network.Messaging.AMQP.FramingTypes
import Network.Messaging.AMQP.Types.Internal

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
