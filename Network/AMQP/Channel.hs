{-# LANGUAGE BangPatterns, ScopedTypeVariables #-}

-- | A connection to an AMQP server is made up of separate
-- channels. It is recommended to use a separate channel for each
-- thread in your application that talks to the AMQP server (but you
-- don't have to as channels are thread-safe)
module Network.AMQP.Channel (
        -- * Opaque channel type
        Channel,

        -- Opening and closing channels
        openChannel, closeChannel',

        -- * Something else
        request,
        readAssembly, writeAssembly
    ) where

import Control.Concurrent ( forkIO, killThread, myThreadId )
import Control.Concurrent.Chan ( Chan, newChan, isEmptyChan
                               , writeChan, readChan )
import Control.Concurrent.MVar ( MVar, newMVar, newEmptyMVar, takeMVar
                               , modifyMVar, modifyMVar_
                               , putMVar, withMVar, tryPutMVar )
import qualified Control.Exception as CE
import qualified Data.IntMap as IM
import qualified Data.Map as M
import Data.Maybe ( isNothing )
import Network.AMQP.Assembly ( readAssembly, writeAssembly, writeAssembly' )
import Network.AMQP.Helpers ( newLock, openLock, closeLock, killLock )
import Network.AMQP.Protocol ( throwMostRelevantAMQPException
                             , msgFromContentHeaderProperties )
import Network.AMQP.Types ( Channel (..), Connection(..), Assembly(..)
                          , MethodPayload(..), ShortString(..), Envelope(..)
                          , AMQPException(..) )

-- | opens a new channel on the connection
--
-- There's currently no closeChannel method, but you can always just
-- close the connection (the maximum number of channels is 65535).
openChannel :: Connection -> IO Channel
openChannel c = do
    newInQueue <- newChan
    outRes <- newChan
    myLastConsumerTag <- newMVar 0
    ca <- newLock

    myChanClosed <- newMVar Nothing
    myConsumers <- newMVar M.empty

    --get a new unused channelID
    newChannelId <- modifyMVar (getLastChannelId c) $ \x -> return (x+1,x+1)

    let newChannel = Channel c newInQueue outRes (fromIntegral newChannelId)
                             myLastConsumerTag ca myChanClosed myConsumers


    thrID <- forkIO $ CE.finally (channelReceiver newChannel)
                                 (closeChannel' newChannel)

    --add new channel to connection's channel map
    modifyMVar_ (getChannels c)
                (\oldMap -> return $ IM.insert newChannelId (newChannel, thrID) oldMap)

    (SimpleMethod (Channel_open_ok _)) <- request newChannel (SimpleMethod (Channel_open (ShortString "")))
    return newChannel


-- | The thread that is run for every channel
channelReceiver :: Channel -> IO ()
channelReceiver chan = do
  --read incoming frames; they are put there by a Connection thread
  p <- readAssembly $ inQueue chan

  if isResponse p
    then do
      emp <- isEmptyChan $ outstandingResponses chan
      if emp
        then CE.throwIO $ userError "got response, but have no corresponding request"
        else do
          x <- readChan (outstandingResponses chan)
          putMVar x p
    --handle asynchronous assemblies
    else handleAsync p

  channelReceiver chan

      where
        isResponse :: Assembly -> Bool
        isResponse (ContentMethod (Basic_deliver _ _ _ _ _) _ _) = False
        isResponse (ContentMethod (Basic_return _ _ _ _) _ _) = False
        isResponse (SimpleMethod (Channel_flow _)) = False
        isResponse (SimpleMethod (Channel_close _ _ _ _)) = False
        isResponse _ = True

        --Basic.Deliver: forward msg to registered consumer
        handleAsync (ContentMethod (Basic_deliver (ShortString consumerTag) deliveryTag redelivered (ShortString myExchangeName)
                                                (ShortString routingKey))
                                properties myMsgBody) =
          withMVar (consumers chan) (\s -> do
            case M.lookup consumerTag s of
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
                  -- got a message, but have no registered subscriber;
                  -- so drop it
                return ()
          )

        handleAsync (SimpleMethod (Channel_close _ (ShortString errorMsg) _ _)) = do

          modifyMVar_ (chanClosed chan) $ \_ -> return $ Just errorMsg
          closeChannel' chan
          killThread =<< myThreadId

        handleAsync (SimpleMethod (Channel_flow active)) = do
          if active
            then openLock $ chanActive chan
            else closeLock $ chanActive chan
        -- in theory we should respond with flow_ok but rabbitMQ 1.7 ignores that, so it doesn't matter
          return ()

        --Basic.return
        handleAsync (ContentMethod (Basic_return _ _ _ _) _ _) = do
            -- TODO: implement handling; this won't be called
            -- currently, because publishMsg sets "mandatory" and
            -- "immediate" to false
          print "BASIC.RETURN not implemented"

-- closes the channel internally; but doesn't tell the server
closeChannel' :: Channel -> IO ()
closeChannel' c = do
  modifyMVar_ (getChannels $ connection c) $ \old -> return $ IM.delete (fromIntegral $ channelID c) old
  -- mark channel as closed
  modifyMVar_ (chanClosed c) $ \x -> do
    killLock $ chanActive c
    killOutstandingResponses $ outstandingResponses c
    return $ Just $ maybe "closed" id x
      where
        killOutstandingResponses :: Chan (MVar a) -> IO ()
        killOutstandingResponses chan = do
          emp <- isEmptyChan chan
          if emp
            then return ()
            else do
              x <- readChan chan
              tryPutMVar x $ error "channel closed"
              killOutstandingResponses chan

-- | sends an assembly and receives the response
request :: Channel -> Assembly -> IO Assembly
request chan m = do
    res <- newEmptyMVar
    CE.catches
          (do
            withMVar (chanClosed chan) $ \cc -> do
              if isNothing cc
                then do
                  writeChan (outstandingResponses chan) res
                  writeAssembly' chan m
                else CE.throwIO $ userError "closed"

           -- res might contain an exception, so evaluate it here
            !r <- takeMVar res
            return r)
          [ CE.Handler (\ (_ :: AMQPException) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.ErrorCall) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.IOException) -> throwMostRelevantAMQPException chan)]
