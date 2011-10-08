{-# LANGUAGE TemplateHaskell #-}

module Network.Messaging.AMQP.Framing (
        -- * Content header
        ContentHeaderProperties(..),
        getContentHeaderProperties, putContentHeaderProperties,

        -- * Methods
        Method(..), MethodPayload(..), getClassIdOf,

        -- * Frame IO
        methodHasContent, peekFrameSize, readFrameSock, writeFrameSock,
        newEmptyAssembler,

        -- * Frames, etc.
        Frame(..), FramePayload(..), Assembler(..),

        -- * Misc
        ChannelId
    ) where

import Control.Applicative ( (<$>), (<*>) )
import Data.Binary ( Binary(..) )
import Data.Binary.Get ( Get, runGet, runGetState,
                            getWord8, getLazyByteString )
import Data.Binary.Put ( Put, runPut, putWord8, putLazyByteString )
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Word ( Word64 )
import Network.Messaging.AMQP.FramingData
import Network.Messaging.AMQP.FramingTypes hiding ( Method )
import Network.Messaging.AMQP.Types.Internal
import Network.Messaging.Helpers ( toLazy, toStrict )
import Network.Socket ( Socket )
import qualified Network.Socket.ByteString as NB

-- | Content header properties used when publishing.
$(genContentHeaderProperties domainMap classes)

-- | Get the numeric class id of the given class.
$(genClassIdFuns classes)
getClassIdOf :: (Num a) => ContentHeaderProperties -> a

-- | The actual identifier and properties for each method.
$(genMethodPayload domainMap classes)

-- | 'Data.Binary.Get' function for content headers.
$(genGetContentHeaderProperties classes)
getContentHeaderProperties :: (Num a) => a -> Get ContentHeaderProperties

-- | 'Data.Binary.Put' function for content headers.
$(genPutContentHeaderProperties classes)
putContentHeaderProperties :: ContentHeaderProperties -> Put

$(genMethodPayloadBinaryInstance domainMap classes)

-- | We represent channel identifiers as 'Int's.  Note that there's also
-- 'ChannelID', which is actually a Word16 and is only used
-- internally.
type ChannelId = Int

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

-- | Represents a "method assembler".  It's effectively a function
-- that takes a frame payload and either outputs another 'Assembler'
-- to run the next frame on, or outputs a complete 'Method' and a new
-- 'Assembler'.  We use closures to model the assembler's state.
newtype Assembler = Assembler (FramePayload ->
                               Either Assembler (Method, Assembler))

-- | A Method is a higher-level object consisting of several frames.
data Method = SimpleMethod MethodPayload
            | ContentMethod MethodPayload ContentHeaderProperties
                            BL.ByteString
              -- ^ method, properties, content-data
              deriving ( Show )

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

-- | Read a frame from the socket.  This will fail with an
-- 'IOException' if the socket is closed or if the frame is
-- fundamentally malformed.
readFrameSock :: Socket -> IO Frame
readFrameSock sock = do
  dat <- recvExact 7
  let len = fromIntegral $ peekFrameSize dat
  dat' <- recvExact (len+1) -- +1 for the terminating 0xCE
  let (frame, _, consumedBytes) = runGetState get (BL.append dat dat') 0

  if consumedBytes /= fromIntegral (len+8)
    then fail $ "readFrameSock: parser should read " ++ show (len + 8) ++
                " bytes; but read " ++ show consumedBytes
    else return ()
  return frame
    where
      recvExact bytes = do
        b <- recvExact' bytes $ BL.empty
        if BL.length b /= fromIntegral bytes
          then fail $ "recvExact wanted " ++ show bytes ++
                      " bytes; got " ++ show (BL.length b) ++ " bytes"
          else return b
      recvExact' bytes buf = do
        dat <- NB.recv sock bytes
        let len = BS.length dat
        if len == 0
          then fail "recv returned 0 bytes"
          else do
            let buf' = BL.append buf (toLazy dat)
            if len >= bytes
              then return buf'
              else recvExact' (bytes-len) buf'

-- | Write a single frame to the socket.
writeFrameSock :: Socket -> Frame -> IO ()
writeFrameSock sock x = do
  NB.send sock $ toStrict $ runPut $ put x
  return ()

-- | Create a new empty 'Assembler'.
newEmptyAssembler :: Assembler
newEmptyAssembler =
    Assembler $ \m@(MethodPayload p) ->
        if methodHasContent m
          then Left (newContentCollector p)
          else Right (SimpleMethod p, newEmptyAssembler)
    where
      newContentCollector :: MethodPayload -> Assembler
      newContentCollector p =
          Assembler $ \(ContentHeaderPayload _ _ bodySize props) ->
              if bodySize > 0
              then Left (bodyContentCollector p props bodySize [])
              else Right (ContentMethod p props BL.empty, newEmptyAssembler)

      bodyContentCollector :: MethodPayload -> ContentHeaderProperties -> Word64
                           -> [BL.ByteString] -> Assembler
      bodyContentCollector p props remData acc =
          Assembler $ \(ContentBodyPayload payload) ->
              let remData' = remData - fromIntegral (BL.length payload)
              in if remData' > 0
                 then Left (bodyContentCollector p props remData'
                                                 (payload:acc))
                 else Right ( ContentMethod p props
                                            (BL.concat $
                                             reverse (payload:acc))
                            , newEmptyAssembler )

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
