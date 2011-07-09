module Network.AMQP.Protocol (
        Frame(..), FramePayload(..),
        methodHasContent, peekFrameSize
    ) where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Lazy.Char8 as BL
import Control.Applicative ( Applicative(..), (<$>) )
import Network.AMQP.Framing
import Network.AMQP.Internal.Types

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

-- | True if a content (content-header and possibly content-body)
-- follows this method
methodHasContent :: FramePayload -> Bool
methodHasContent (MethodPayload (Basic_get_ok _ _ _ _ _))  = True
methodHasContent (MethodPayload (Basic_deliver _ _ _ _ _)) = True
methodHasContent (MethodPayload (Basic_return _ _ _ _))    = True
methodHasContent _                                         = False

-- | Get the size of the frame.
-- Pre: the argument is at least 7 bytes long
peekFrameSize :: BL.ByteString -> PayloadSize
peekFrameSize = runGet $ do
                  getWord8            -- 1 byte
                  get :: Get ChannelID -- 2 bytes
                  return =<< get      -- 4 bytes

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
