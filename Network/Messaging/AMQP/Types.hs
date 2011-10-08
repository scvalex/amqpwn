{-# LANGUAGE DeriveDataTypeable #-}

module Network.Messaging.AMQP.Types (
        module Network.Messaging.AMQP.Types.Internal,
        module Network.Messaging.AMQP.Framing,

        -- * AMQP high-level types
        Connection(..), Channel(..), ChannelType(..),

        -- * Convenience types
        QueueName, ExchangeName, ExchangeType, RoutingKey, MessageId,

        -- * AMQP Exceptions
        AMQPException(..)
    ) where

import Control.Concurrent ( ThreadId )
import Control.Concurrent.STM ( TVar, TMVar )
import Control.Exception ( Exception )
import Data.Int ( Int64 )
import Data.IntMap ( IntMap )
import Data.Typeable ( Typeable )
import Network.Socket ( Socket )
import Network.Messaging.AMQP.Framing
import Network.Messaging.AMQP.Types.Internal


-- High-level types

-- | Represents an AMQP connection.
data Connection = Connection
    { getSocket :: TMVar Socket
      -- ^ connection socket
    , getMaxFrameSize :: Int64
      -- ^ negotiated maximum frame size
    , getConnClosed :: TMVar AMQPException
      -- ^ reason for closure, if closed
    , getConnCloseHandlers :: TVar [AMQPException -> IO ()]
      -- ^ handlers to be notified of connection closures
    , getChannels :: TVar (IntMap Channel)
      -- ^ open channels ('ChannelId' => 'Channel')
    , getLastChannelId :: TVar ChannelId
      -- ^ for auto-incrementing the 'ChannelId's
    }

-- | Represents an AMQP channel.
data Channel = Channel
    { getAssembler :: TVar Assembler
      -- ^ method assembler
    , getChanClosed :: TMVar AMQPException
      -- ^ reason for closing the channel
    , getConsumer :: TMVar (Method -> IO ())
      -- ^ consumer callback
    , getChannelType :: ChannelType
      -- ^ what the channel is used for
    , getChannelRPC :: TMVar Method
      -- ^ holder for the channel's control command result
    }

-- | What is the channel used for?  Control commands?  Publishing?
data ChannelType = ControlChannel
                 | PublishingChannel { getPublishingThreadId :: ThreadId
                                     , getBasicAckHandler :: ()
                                     , getBasicNackHandler :: ()
                                     , getBasicReturnHandler :: ()
                                     }
                   deriving ( Eq )

-- Convenience types

-- | We represent queue names as 'String's.
type QueueName = String

-- | We represent exchange names as 'String's.
type ExchangeName = String

-- | We represent exchange types as 'String's.  We could use a
-- datatype here, but since the meaning of the exchange's type is
-- defined by the broker, we'll just leave it free-form.
type ExchangeType = String

-- | Routing keys are also 'String's.
type RoutingKey = String

-- | Message ids are just plain 'Int's.
type MessageId = Int

-- Exceptions

-- | The Exception thrown when soft and hard AQMP errors occur.
data AMQPException = ChannelClosedException String
                   | ConnectionClosedException String
                   | ConnectionStartException String
                   | ClientException String
                     deriving (Typeable, Show, Ord, Eq)

instance Exception AMQPException
