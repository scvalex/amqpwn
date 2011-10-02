{-# LANGUAGE DeriveDataTypeable #-}

module Network.AMQP.Types (
        module Network.AMQP.Types.Internal,
        module Network.AMQP.Framing,

        -- * AMQP high-level types
        Connection(..), Channel(..), Assembler(..),
        ChannelId, ChannelType(..),

        -- * Convenience types
        QueueName, ExchangeName, ExchangeType, RoutingKey,

        -- * Message payload
        Method(..), Frame(..), FramePayload(..),

        -- * AMQP Exceptions
        AMQPException(..)
    ) where

import Control.Applicative ( Applicative(..), (<$>) )
import Control.Concurrent ( ThreadId )
import Control.Concurrent.STM ( TVar, TMVar )
import Control.Exception ( Exception )
import Data.Binary ( Binary(..) )
import Data.Binary.Get ( Get, getWord8, getLazyByteString )
import Data.Binary.Put ( Put, runPut, putWord8, putLazyByteString )
import Data.ByteString.Lazy.Char8 ( ByteString )
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Int ( Int64 )
import Data.IntMap ( IntMap )
import Data.Typeable ( Typeable )
import Network.Socket ( Socket )
import Network.AMQP.Framing
import Network.AMQP.Types.Internal


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

-- | Represents a "method assembler".  It's effectively a function
-- that takes a frame payload and either outputs another 'Assembler'
-- to run the next frame on, or outputs a complete 'Method' and a new
-- 'Assembler'.  We use closures to model the assembler's state.
newtype Assembler = Assembler (FramePayload -> Either Assembler (Method, Assembler))

-- | We represent channel identifiers as 'Int's.  Note that there's also
-- 'ChannelID', which is actually a Word16 and is only used
-- internally.
type ChannelId = Int

-- | What is the channel used for?  Control commands?  Publishing?
data ChannelType = ControlChannel | PublishingChannel ThreadId
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

-- Message payload

-- | A Method is a higher-level object consisting of several frames.
data Method = SimpleMethod MethodPayload
            | ContentMethod MethodPayload ContentHeaderProperties ByteString
              -- ^ method, properties, content-data
              deriving ( Show )

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
                  -- ^ classId, weight, bodySize, propertyFields
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

-- | The Exception thrown when soft and hard AQMP errors occur.
data AMQPException = ChannelClosedException String
                   | ConnectionClosedException String
                   | ConnectionStartException String
                   | ClientException String
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
