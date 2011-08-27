{-# LANGUAGE ScopedTypeVariables #-}

module Network.AMQP.Protocol (
        methodHasContent, peekFrameSize, readFrameSock, writeFrameSock,
        msgFromContentHeaderProperties, writeFrames, newEmptyAssembler
    ) where

import qualified Control.Exception as CE
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import Network.AMQP.Helpers ( toLazy, toStrict, withTMVarIO )
import Network.AMQP.Framing
import Network.AMQP.Types
import Network.Socket ( Socket )
import qualified Network.Socket.ByteString as NB
import Text.Printf ( printf )


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

readFrameSock :: Socket -> Int -> IO Frame
readFrameSock sock _ = do
  dat <- recvExact 7
  let len = fromIntegral $ peekFrameSize dat
  dat' <- recvExact (len+1) -- +1 for the terminating 0xCE
  let (frame, _, consumedBytes) = runGetState get (BL.append dat dat') 0

  if consumedBytes /= fromIntegral (len+8)
    then error $ "readFrameSock: parser should read " ++ show (len + 8) ++
                 " bytes; but read " ++ show consumedBytes
    else return ()
  return frame
    where
      recvExact bytes = do
        b <- recvExact' bytes $ BL.empty
        if BL.length b /= fromIntegral bytes
          then error $ "recvExact wanted " ++ show bytes ++
                       " bytes; got " ++ show (BL.length b) ++ " bytes"
          else return b
      recvExact' bytes buf = do
        dat <- NB.recv sock bytes
        let len = BS.length dat
        if len == 0
          then CE.throwIO $ ConnectionClosedException "recv returned 0 bytes"
          else do
            let buf' = BL.append buf (toLazy dat)
            if len >= bytes
              then return buf'
              else recvExact' (bytes-len) buf'

writeFrameSock :: Socket -> Frame -> IO ()
writeFrameSock sock x = do
  NB.send sock $ toStrict $ runPut $ put x
  return ()

writeFrames :: Connection -> ChannelId -> [FramePayload] -> IO ()
writeFrames conn chId payloads = do
  withTMVarIO (getSocket conn) $ \sock -> do
      mapM_ (\payload -> writeFrameSock sock (Frame (fromIntegral chId) payload))
            payloads
      `CE.catch`
      (\(e :: CE.IOException) -> CE.throw . ClientException $
                                  printf "IOException on %d: %s" chId (show e))

msgFromContentHeaderProperties :: ContentHeaderProperties -> BL.ByteString
                               -> Message
msgFromContentHeaderProperties (CHBasic content_type _ _ delivery_mode _ correlation_id reply_to _ message_id timestamp _ _ _ _) myMsgBody =
    let msgId = fromShortString message_id
        contentType = fromShortString content_type
        replyTo = fromShortString reply_to
        correlationID = fromShortString correlation_id
    in Message myMsgBody (fmap intToDeliveryMode delivery_mode) timestamp msgId contentType replyTo correlationID
        where
          fromShortString (Just (ShortString s)) = Just s
          fromShortString _ = Nothing

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
                 then Left (bodyContentCollector p props remData' (payload:acc))
                 else Right ( ContentMethod p props
                                            (BL.concat $ reverse (payload:acc))
                            , newEmptyAssembler )
