-- -*- haskell-mode -*-

{-# LANGUAGE TemplateHaskell #-}

module Network.AMQP.Framing where

import Network.AMQP.FramingData
import Network.AMQP.FramingTypes
import Network.AMQP.Types

import Data.Binary ( Binary(..) )

$(genContentHeaderProperties domainMap classes)

$(genClassIDFuns classes)

$(genMethodPayload domainMap classes)

$(genGetContentHeaderProperties classes)

$(genPutContentHeaderProperties classes)

$(genMethodPayloadBinaryInstance domainMap classes)
