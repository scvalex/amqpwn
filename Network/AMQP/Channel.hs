{-# LANGUAGE BangPatterns, ScopedTypeVariables #-}

-- | A connection to an AMQP server is made up of separate
-- channels. It is recommended to use a separate channel for each
-- thread in your application that talks to the AMQP server (but you
-- don't have to as channels are thread-safe)
module Network.AMQP.Channel (
        -- * Opaque channel type
        Channel,

        -- Opening and closing channels
        openChannel, closeChannel, closeChannelNormal,

        -- * Something else
        request,
        readMethod, writeMethod
    ) where

import Control.Applicative ( (<$>) )
import Control.Concurrent ( forkIO, killThread, myThreadId )
import Control.Concurrent.STM ( STM, atomically, TChan, newTChan, isEmptyTChan
                              , writeTChan, readTChan, TMVar, newTMVar
                              , newEmptyTMVar, takeTMVar, putTMVar, readTMVar
                              , tryPutTMVar, isEmptyTMVar )
import qualified Control.Exception as CE
import qualified Data.ByteString.Lazy.Char8 as LB
import qualified Data.IntMap as IM
import qualified Data.Map as M
import Data.String ( IsString(..) )
import Network.AMQP.Protocol ( methodHasContent, collectContent, writeFrames
                             , throwMostRelevantAMQPException
                             , msgFromContentHeaderProperties )
import Network.AMQP.Types ( Channel (..), Connection(..), Method(..)
                          , MethodPayload(..), ShortString(..), Envelope(..)
                          , AMQPException(..), FramePayload(..)
                          , getClassIDOf )
import Text.Printf ( printf )

-- | Open a new channel on the connection.
openChannel :: Connection -> IO Channel
openChannel conn = do
    newChannel <- atomically $ do
        newInQueue <- newTChan
        rpcQueue <- newTChan
        myLastConsumerTag <- newTMVar 0

        myChanClosed <- newEmptyTMVar
        myConsumers <- newTMVar M.empty

        -- get a new unused channelID
        newChannelId <- (+1) <$> takeTMVar (getLastChannelId conn)
        putTMVar (getLastChannelId conn) newChannelId

        return $ Channel { getConnection           = conn
                         , getInQueue              = newInQueue
                         , getRPCQueue             = rpcQueue
                         , getChannelId            = fromIntegral newChannelId
                         , getLastConsumerTag      = myLastConsumerTag
                         , getChanClosed           = myChanClosed
                         , getConsumers            = myConsumers }

    tid <- forkIO $ CE.finally (channelReceiver newChannel)
                               (closeChannel' newChannel)

    -- add new channel to connection's channel map
    atomically $ do
      channels <- takeTMVar (getChannels conn)
      putTMVar (getChannels conn) $
               IM.insert (fromIntegral $ getChannelId newChannel)
                         (newChannel, tid)
                         channels

    (SimpleMethod (Channel_open_ok _)) <-
        request newChannel . SimpleMethod $ Channel_open (fromString "")
    return newChannel

-- | Process: Maintains the incoming method queue for the channel.
channelReceiver :: Channel -> IO ()
channelReceiver chan = do
  -- read incoming frames; they are put there by a Connection thread
  p <- readMethod $ getInQueue chan
  if isResponse p
    then atomically $ do
      emp <- isEmptyTChan $ getRPCQueue chan
      if emp
        then CE.throw . ConnectionClosedException $
             printf "got unrequested response: %s" (show p)
        else flip putTMVar p =<< readTChan (getRPCQueue chan)
    -- handle asynchronous methods
    else handleAsync p

  channelReceiver chan
      where
        isResponse :: Method -> Bool
        isResponse (ContentMethod (Basic_deliver _ _ _ _ _) _ _) = False
        isResponse (ContentMethod (Basic_return _ _ _ _) _ _)    = False
        isResponse (SimpleMethod (Channel_flow _))               = False
        isResponse (SimpleMethod (Channel_close _ _ _ _))        = False
        isResponse (SimpleMethod (Basic_ack _ _))                = False
        isResponse (SimpleMethod (Basic_nack _ _ _))             = False
        isResponse (SimpleMethod (Basic_cancel _ _))             = False
        isResponse _                                             = True

        -- basic.deliver: forward message to registered consumer
        handleAsync (ContentMethod (Basic_deliver (ShortString consumerTag) deliveryTag redelivered (ShortString myExchangeName)
                                                (ShortString routingKey))
                                properties myMsgBody) = do
          consumers <- atomically $ readTMVar (getConsumers chan)
          case M.lookup consumerTag consumers of
            Just subscriber -> do
              let msg = msgFromContentHeaderProperties properties myMsgBody
                  env = Envelope { envDeliveryTag = deliveryTag
                                 , envRedelivered = redelivered
                                 , envExchangeName = myExchangeName
                                 , envRoutingKey = routingKey
                                 , envChannel = chan
                                 }
              subscriber (msg, env)
            Nothing -> do
              -- got a message, but have no registered subscriber, so
              -- drop it
              return ()

        handleAsync (SimpleMethod (Channel_close _ (ShortString errorMsg) _ _)) = do

          atomically $ putTMVar (getChanClosed chan)
                                (ChannelClosedException errorMsg)
          unsafeWriteMethod chan (SimpleMethod Channel_close_ok)
          closeChannel' chan
          killThread =<< myThreadId

        handleAsync (ContentMethod (Basic_return _ _ _ _) _ _) = do
            -- TODO: implement handling; this won't be called
            -- currently, because publishMsg sets "mandatory" and
            -- "immediate" to false
          CE.throw $ ConnectionClosedException "basic.return not implemented"

        handleAsync val = do
          CE.throw . ConnectionClosedException $
            printf "not implemented: %s" (show val)

-- | Initiate a normal channel close.
closeChannelNormal :: Channel -> IO ()
closeChannelNormal = closeChannel "Normal"

-- | Initiate a channel close.
closeChannel :: String -> Channel -> IO ()
closeChannel reason chan = do
  request chan . SimpleMethod $
          Channel_close 200 (fromString reason) 0 0
  atomically $ putTMVar (getChanClosed chan) (ChannelClosedException reason)
  closeChannel' chan

-- | Close the channel internally, but doen't tell the server.  Note
-- that this hangs if @getChanClosed@ is not already set.
closeChannel' :: Channel -> IO ()
closeChannel' chan = atomically $ do
  channels <- takeTMVar (getChannels $ getConnection chan)
  putTMVar (getChannels $ getConnection chan) $
       IM.delete (fromIntegral $ getChannelId chan) channels
  -- mark channel as closed
  chanClosed <- takeTMVar (getChanClosed chan)
  killRPCQueue chanClosed $ getRPCQueue chan
  putTMVar (getChanClosed chan) chanClosed
    where
      killRPCQueue :: AMQPException -> TChan (TMVar a) -> STM ()
      killRPCQueue reason queue = do
        emp <- isEmptyTChan queue
        if emp
          then return ()
          else do
            x <- readTChan queue
            tryPutTMVar x $ CE.throw reason
            killRPCQueue reason queue

-- | Send a method and wait for response.
request :: Channel -> Method -> IO Method
request chan m = do
    res <- atomically $ newEmptyTMVar
    CE.catches
          (do
            notClosed <- atomically $ isEmptyTMVar (getChanClosed chan)
            if notClosed
              then do
                atomically $ writeTChan (getRPCQueue chan) res
                unsafeWriteMethod chan m
              else do
                CE.throw =<< atomically (readTMVar $ getChanClosed chan)

            -- res might contain an exception, so evaluate it here
            !r <- atomically $ takeTMVar res
            return r)
          [ CE.Handler (\ (_ :: AMQPException) ->
                            throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.ErrorCall) ->
                            throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.IOException) ->
                            throwMostRelevantAMQPException chan)]

-- | Read an entire method (multiple frames) and return it.
readMethod :: TChan FramePayload -> IO Method
readMethod chan = do
  m <- atomically $ readTChan chan
  case m of
    MethodPayload p ->           -- got a method frame
         if methodHasContent m
           then do
             (props, msg) <- collectContent chan
             return $ ContentMethod p props msg
           else do
             return $ SimpleMethod p
    unframe -> CE.throw . ConnectionClosedException $
                  printf "unexpected frame: %s" (show unframe)

-- | Write a method to the channel.  Normalizes exceptions.
writeMethod :: Channel -> Method -> IO ()
writeMethod chan m =
    CE.catches
          (unsafeWriteMethod chan m)
          [ CE.Handler (\ (_ :: AMQPException) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.ErrorCall) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.IOException) -> throwMostRelevantAMQPException chan)
          ]

-- | Write a method to the channel.  Does /not/ handle exceptions.
unsafeWriteMethod :: Channel -> Method -> IO ()
unsafeWriteMethod chan (ContentMethod m properties msg) = do
  let !toWrite = [ MethodPayload m
                 , ContentHeaderPayload (getClassIDOf properties) --classID
                                        0 --weight is deprecated in AMQP 0-9
                                        (fromIntegral $ LB.length msg) --bodySize
                                        properties
                 ] ++
                 (if LB.length msg > 0
                    then do
                      --split into frames of maxFrameSize
                      map ContentBodyPayload
                         (splitLen msg (fromIntegral $ getMaxFrameSize $ getConnection chan))
                    else [])
  writeFrames chan toWrite
      where
        splitLen str len | LB.length str > len = (LB.take len str):(splitLen (LB.drop len str) len)
        splitLen str _ = [str]

unsafeWriteMethod chan (SimpleMethod m) = do
    writeFrames chan [MethodPayload m]
