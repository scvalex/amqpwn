{-# LANGUAGE BangPatterns, ScopedTypeVariables #-}

-- | A connection to an AMQP server is made up of separate
-- channels. It is recommended to use a separate channel for each
-- thread in your application that talks to the AMQP server (but you
-- don't have to as channels are thread-safe)
module Network.AMQP.Channel (
        -- * Opaque channel type
        Channel,

        -- Opening and closing channels
        openChannel,

        -- * Something else
        request,
        readAssembly, writeAssembly
    ) where

import Control.Applicative ( (<$>) )
import Control.Concurrent ( forkIO, killThread, myThreadId )
import Control.Concurrent.STM ( STM, atomically, TChan, newTChan, isEmptyTChan
                              , writeTChan, readTChan, TMVar, newTMVar
                              , newEmptyTMVar, takeTMVar, putTMVar, readTMVar
                              , tryPutTMVar )
import qualified Control.Exception as CE
import qualified Data.IntMap as IM
import qualified Data.Map as M
import Data.Maybe ( isNothing )
import Data.String ( fromString )
import Network.AMQP.Assembly ( readAssembly, writeAssembly, writeAssembly' )
import Network.AMQP.Helpers ( newLock, openLock, closeLock, killLock )
import Network.AMQP.Protocol ( throwMostRelevantAMQPException
                             , msgFromContentHeaderProperties )
import Network.AMQP.Types ( Channel (..), Connection(..), Assembly(..)
                          , MethodPayload(..), ShortString(..), Envelope(..)
                          , AMQPException(..) )
import Text.Printf ( printf )

-- | Open a new channel on the connection.
--
-- FIXME: Implement channel.close.
openChannel :: Connection -> IO Channel
openChannel conn = do
    newChannel <- atomically $ do
        newInQueue <- newTChan
        rpcQueue <- newTChan
        myLastConsumerTag <- newTMVar 0
        ca <- newLock

        myChanClosed <- newTMVar Nothing
        myConsumers <- newTMVar M.empty

        -- get a new unused channelID
        newChannelId <- (+1) <$> takeTMVar (getLastChannelId conn)
        putTMVar (getLastChannelId conn) newChannelId

        return $ Channel { getConnection           = conn
                         , getInQueue              = newInQueue
                         , getRPCQueue             = rpcQueue
                         , getChannelId       = fromIntegral newChannelId
                         , getLastConsumerTag      = myLastConsumerTag
                         , getChanActive           = ca
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
  p <- readAssembly $ getInQueue chan

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
        isResponse :: Assembly -> Bool
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

          atomically $ putTMVar (getChanClosed chan) (Just errorMsg)
          closeChannel' chan
          killThread =<< myThreadId

        handleAsync (SimpleMethod (Channel_flow active)) = atomically $ do
          if active
            then openLock $ getChanActive chan
            else closeLock $ getChanActive chan
        -- in theory we should respond with flow_ok but rabbitMQ 1.7 ignores that, so it doesn't matter
          return ()

        --Basic.return
        handleAsync (ContentMethod (Basic_return _ _ _ _) _ _) = do
            -- TODO: implement handling; this won't be called
            -- currently, because publishMsg sets "mandatory" and
            -- "immediate" to false
          print "BASIC.RETURN not implemented"

-- | Close the channel internally, but doen't tell the server.  Note
-- that this hangs if getChanClosed is not already set.
closeChannel' :: Channel -> IO ()
closeChannel' chan = atomically $ do
  channels <- takeTMVar (getChannels $ getConnection chan)
  putTMVar (getChannels $ getConnection chan) $
       IM.delete (fromIntegral $ getChannelId chan) channels
  -- mark channel as closed
  chanClosed <- takeTMVar (getChanClosed chan)
  killLock $ getChanActive chan
  killRPCQueue $ getRPCQueue chan
  putTMVar (getChanClosed chan) (Just $ maybe "closed" id chanClosed)
    where
      killRPCQueue :: TChan (TMVar a) -> STM ()
      killRPCQueue queue = do
        emp <- isEmptyTChan queue
        if emp
          then return ()
          else do
            x <- readTChan queue
            tryPutTMVar x . CE.throw $ ChannelClosedException "closed"
            killRPCQueue queue

-- | Send a method and wait for response.
request :: Channel -> Assembly -> IO Assembly
request chan m = do
    res <- atomically $ newEmptyTMVar
    CE.catches
          (do
            chanClosed <- atomically $ readTMVar (getChanClosed chan)
            if isNothing chanClosed
              then do
                atomically $ writeTChan (getRPCQueue chan) res
                writeAssembly' chan m
              else CE.throw $ ChannelClosedException "closed"

            -- res might contain an exception, so evaluate it here
            !r <- atomically $ takeTMVar res
            return r)
          [ CE.Handler (\ (_ :: AMQPException) ->
                            throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.ErrorCall) ->
                            throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.IOException) ->
                            throwMostRelevantAMQPException chan)]
