module Network.AMQP.Types (
        -- * AMQP high-level types
        Connection(..), Channel(..),
        -- * Message/Envelope
        Assembly(..), Message(..), newMsg, Envelope(..),
        DeliveryMode(..), deliveryModeToInt, intToDeliveryMode
    ) where

import Control.Concurrent ( MVar, ThreadId, Chan )
import Data.ByteString.Lazy.Char8 ( ByteString, empty )
import Data.IntMap ( IntMap )
import qualified Data.Map as M
import Data.Word ( Word16 )
import Network.AMQP.Helpers ( Lock )
import Network.Socket ( Socket )
import Network.AMQP.Framing ( MethodPayload, ContentHeaderProperties )
import Network.AMQP.Internal.Types ( Timestamp, LongLongInt )
import Network.AMQP.Protocol ( FramePayload(..) )


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
