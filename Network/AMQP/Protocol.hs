{-# LANGUAGE ScopedTypeVariables #-}

module Network.AMQP.Protocol (
        methodHasContent, peekFrameSize, readFrameSock, writeFrameSock,
        collectContent, msgFromContentHeaderProperties, writeFrames,
        throwMostRelevantAMQPException
    ) where

import Control.Concurrent.STM ( atomically, TChan, readTChan, readTMVar
                              , isEmptyTMVar )
import qualified Control.Exception as CE
import Data.Binary
import Data.Binary.Get
import qualified Data.IntMap as IntMap
import Data.Binary.Put
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import Network.AMQP.Helpers ( toLazy, toStrict )
import Network.AMQP.Framing
import Network.AMQP.Internal.Types
import Network.AMQP.Types
import Network.Socket ( Socket )
import qualified Network.Socket.ByteString as NB


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

-- | writes multiple frames to the channel atomically
writeFrames :: Channel -> [FramePayload] -> IO ()
writeFrames chan payloads =
    let conn = getConnection chan
    in do
      chans <- atomically $ readTMVar (getChannels conn)
      if IntMap.member (fromIntegral $ getChannelId chan) chans
        then CE.catch
             -- ensure at most one thread is writing to the socket
             -- at any time
                 (atomically (readTMVar $ getConnWriteLock conn) >>= \_ ->
                      mapM_ (\payload -> writeFrameSock (getSocket conn) (Frame (getChannelId chan) payload)) payloads)
                 ( \(_ :: CE.IOException) -> do
                     CE.throwIO $ userError "connection not open")
        else do
          CE.throwIO $ userError "channel not open"

-- | reads a contentheader and contentbodies and assembles them
collectContent :: TChan FramePayload -> IO (ContentHeaderProperties, BL.ByteString)
collectContent chan = do
  (ContentHeaderPayload _ _ bodySize props) <- atomically $ readTChan chan

  content <- collect $ fromIntegral bodySize
  return (props, BL.concat content)
      where
        collect x | x <= 0 = return []
        collect remData = do
          (ContentBodyPayload payload) <- atomically $ readTChan chan
          r <- collect (remData - (BL.length payload))
          return $ payload : r

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

-- Throws an AMQPException based on the status of the connection and
-- of the channel.  If both connection and channel are closed, throw
-- the connection's exception.  Otherwise throw the channel's
-- exception.  If neither are closed, throw a ClientExeption.
throwMostRelevantAMQPException :: Channel -> IO b
throwMostRelevantAMQPException chan = atomically $ do
  notConnClosed <- isEmptyTMVar (getConnClosed $ getConnection chan)
  if notConnClosed
     then do
       dontHaveReason <- isEmptyTMVar $ getChanClosed chan
       if dontHaveReason
         then CE.throw $ ClientException "Unknown Reason"
         else CE.throw =<< readTMVar (getChanClosed chan)
     else do
       CE.throw =<< readTMVar (getConnClosed $ getConnection chan)
