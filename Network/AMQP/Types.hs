{-# LANGUAGE DeriveDataTypeable #-}

module Network.AMQP.Types (
        module Network.AMQP.Internal.Types,
        module Network.AMQP.Framing,

        -- * AMQP high-level types
        Connection(..), Channel(..),

        -- * Message/Envelope
        Assembly(..), Message(..), newMsg, Envelope(..),
        DeliveryMode(..), deliveryModeToInt, intToDeliveryMode,

        -- * Message payload
        Frame(..), FramePayload(..),

        -- * AMQP Exceptions
        AMQPException(..)
    ) where

import Control.Applicative ( Applicative(..), (<$>) )
import Control.Concurrent ( MVar, ThreadId, Chan )
import Control.Exception ( Exception )
import Data.Binary ( Binary(..) )
import Data.Binary.Get ( Get, getWord8, getLazyByteString )
import Data.Binary.Put ( Put, runPut, putWord8, putLazyByteString )
import Data.ByteString.Lazy.Char8 ( ByteString, empty )
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.IntMap ( IntMap )
import qualified Data.Map as M
import Data.Typeable ( Typeable )
import Data.Word ( Word16 )
import Network.AMQP.Helpers ( Lock )
import Network.Socket ( Socket )
import Network.AMQP.Framing
import Network.AMQP.Internal.Types


-- High-level types

-- | Represents an AMQP connection.
data Connection = Connection {
      connSocket :: Socket,
      connChannels :: (MVar (IntMap (Channel, ThreadId))), --open channels (channelID => (Channel, ChannelThread))
      connMaxFrameSize :: Int, --negotiated maximum frame size
      connClosed :: MVar (Maybe String),
      connClosedLock :: MVar (), -- used by closeConnection to block until connection-close handshake is complete
      connWriteLock :: MVar (), -- to ensure atomic writes to the socket
      connClosedHandlers :: MVar [IO ()],
      lastChannelID :: MVar Int --for auto-incrementing the channelIDs
    }

-- | Represents an AMQP channel.
data Channel = Channel {
      connection :: Connection,
      inQueue :: Chan FramePayload, --incoming frames (from Connection)
      outstandingResponses :: Chan (MVar Assembly), -- for every request an MVar is stored here waiting for the response
      channelID :: Word16,
      lastConsumerTag :: MVar Int,
      chanActive :: Lock, -- used for flow-control. if lock is closed, no content methods will be sent
      chanClosed :: MVar (Maybe String),
      consumers :: MVar (M.Map String ((Message, Envelope) -> IO ())) -- who is consumer of a queue? (consumerTag => callback)
    }

-- | An assembly is a higher-level object consisting of several frames
-- (like in amqp 0-10)
data Assembly = SimpleMethod MethodPayload
              | ContentMethod MethodPayload ContentHeaderProperties ByteString --method, properties, content-data
                deriving ( Show )

-- Message/Envelope

-- | An AMQP message
data Message = Message {
      msgBody :: ByteString, -- ^ the content of your message
      msgDeliveryMode :: Maybe DeliveryMode, -- ^ see 'DeliveryMode'
      msgTimestamp :: Maybe Timestamp, -- ^ use in any way you like; this doesn't affect the way the message is handled
      msgID :: Maybe String, -- ^ use in any way you like; this doesn't affect the way the message is handled
      msgContentType :: Maybe String,
      msgReplyTo :: Maybe String,
      msgCorrelationID :: Maybe String
    } deriving ( Show )

-- | A new 'Message' with defaults set; you should override at least
-- 'msgBody'.
newMsg :: Message
newMsg = Message empty Nothing Nothing Nothing Nothing Nothing Nothing

-- | Contains meta-information of a delivered message (through
-- 'getMsg' or 'consumeMsgs').
data Envelope = Envelope {
      envDeliveryTag :: LongLongInt,
      envRedelivered :: Bool,
      envExchangeName :: String,
      envRoutingKey :: String,
      envChannel :: Channel
    }

data DeliveryMode = Persistent -- ^ the message will survive server restarts (if the queue is durable)
                  | NonPersistent -- ^ the message may be lost after server restarts
                    deriving ( Show )

deliveryModeToInt :: (Num a) => DeliveryMode -> a
deliveryModeToInt NonPersistent = 1
deliveryModeToInt Persistent = 2

intToDeliveryMode :: (Num a) => a -> DeliveryMode
intToDeliveryMode 1 = NonPersistent
intToDeliveryMode 2 = Persistent

-- Message payload

-- | A frame received on a channel
data Frame = Frame ChannelID FramePayload -- ^ channel, payload
             deriving ( Show )

instance Binary Frame where
    get = do
      thisFrameType <- getWord8
      channelId <- get :: Get ChannelID
      payloadSize <- get :: Get PayloadSize
      payload <- getPayload (toEnum $ fromIntegral thisFrameType) payloadSize
      0xCE <- getWord8           -- frame end
      return $ Frame channelId payload
    put (Frame channelId payload) = do
      putWord8 . fromIntegral $ fromEnum payload
      put channelId
      let buf = runPut $ putPayload payload
      put ((fromIntegral $ BL.length buf) :: PayloadSize)
      putLazyByteString buf
      putWord8 0xCE             -- frame end

-- | A frame's payload
data FramePayload = MethodPayload MethodPayload
                  | ContentHeaderPayload ShortInt ShortInt LongLongInt
                                         ContentHeaderProperties
                  -- ^ classID, weight, bodySize, propertyFields
                  | ContentBodyPayload BL.ByteString
                    deriving ( Show )

instance Enum FramePayload where
    fromEnum (MethodPayload _)              = 1
    fromEnum (ContentHeaderPayload _ _ _ _) = 2
    fromEnum (ContentBodyPayload _)         = 3
    toEnum 1 = MethodPayload { }
    toEnum 2 = ContentHeaderPayload { }
    toEnum 3 = ContentBodyPayload { }

-- Exceptions

-- | The Exception thrown when soft and hard AQMP error occur.
data AMQPException = ChannelClosedException String
                   | ConnectionClosedException String
                     deriving (Typeable, Show, Ord, Eq)

instance Exception AMQPException

-- Internal Helpers
-- | Get a the given method's payload.
-- FIXME: Fill in the given method rather than building a new one.
getPayload :: (Integral n) => FramePayload -> n -> Get FramePayload
getPayload (MethodPayload _) _ = do
  MethodPayload <$> get
getPayload (ContentHeaderPayload _ _ _ _) _ = do
  classID <- get :: Get ShortInt
  ContentHeaderPayload <$> return classID
                       <*> get
                       <*> get
                       <*> getContentHeaderProperties classID
getPayload (ContentBodyPayload _) payloadSize = do
  ContentBodyPayload <$> (getLazyByteString $ fromIntegral payloadSize)

-- | Put a frame's payload.
putPayload :: FramePayload -> Put
putPayload (MethodPayload payload) =
    put payload
putPayload (ContentHeaderPayload classID weight bodySize p) =
    put classID >> put weight >> put bodySize >> putContentHeaderProperties p
putPayload (ContentBodyPayload payload) =
    putLazyByteString payload
