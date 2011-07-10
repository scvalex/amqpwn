{-# LANGUAGE BangPatterns, ScopedTypeVariables #-}

-- | A connection to an AMQP server is made up of separate
-- channels. It is recommended to use a separate channel for each
-- thread in your application that talks to the AMQP server (but you
-- don't have to as channels are thread-safe)
module Network.AMQP.Channel (
        Channel,
        openChannel, closeChannel', request,
        readAssembly, writeAssembly
    ) where

import Prelude hiding ( lookup, length, take, drop )

import Control.Concurrent ( forkIO, killThread, myThreadId )
import Control.Concurrent.Chan ( Chan, newChan, isEmptyChan
                               , writeChan, readChan )
import Control.Concurrent.MVar ( MVar, newMVar, newEmptyMVar, takeMVar
                               , modifyMVar, modifyMVar_, readMVar
                               , putMVar, withMVar, tryPutMVar )
import qualified Control.Exception as CE
import Data.ByteString.Lazy.Char8 ( length, take, drop )
import Data.IntMap ( insert, delete, member )
import Data.Map ( empty, lookup )
import Data.Maybe ( isNothing )
import Network.AMQP.Helpers ( newLock, openLock, closeLock, killLock
                            , waitLock )
import Network.AMQP.Protocol ( methodHasContent, collectContent
                             , writeFrameSock
                             , msgFromContentHeaderProperties )
import Network.AMQP.Types ( Channel (..), Connection(..), Assembly(..)
                          , MethodPayload(..), ShortString(..), Envelope(..)
                          , AMQPException, FramePayload(..), getClassIDOf
                          , AMQPException(..), Frame(..) )

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
    myConsumers <- newMVar empty

    --get a new unused channelID
    newChannelID <- modifyMVar (lastChannelID c) $ \x -> return (x+1,x+1)

    let newChannel = Channel c newInQueue outRes (fromIntegral newChannelID)
                             myLastConsumerTag ca myChanClosed myConsumers


    thrID <- forkIO $ CE.finally (channelReceiver newChannel)
                                 (closeChannel' newChannel)

    --add new channel to connection's channel map
    modifyMVar_ (connChannels c)
                (\oldMap -> return $ insert newChannelID (newChannel, thrID) oldMap)

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
            case lookup consumerTag s of
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
  modifyMVar_ (connChannels $ connection c) $ \old -> return $ delete (fromIntegral $ channelID c) old
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

------------- ASSEMBLY -------------------------
-- | reads all frames necessary to build an assembly
readAssembly :: Chan FramePayload -> IO Assembly
readAssembly chan = do
  m <- readChan chan
  case m of
    MethodPayload p -> --got a method frame
      if methodHasContent m
        then do
          --several frames containing the content will follow, so read them
          (props, msg) <- collectContent chan
          return $ ContentMethod p props msg
        else do
          return $ SimpleMethod p
    x -> error $ "didn't expect frame: " ++ (show x)

writeAssembly' :: Channel -> Assembly -> IO ()
writeAssembly' chan (ContentMethod m properties msg) = do
  -- wait iff the AMQP server instructed us to withhold sending
  -- content data (flow control)
  waitLock $ chanActive chan
  let !toWrite = [ MethodPayload m
                 , ContentHeaderPayload (getClassIDOf properties) --classID
                                        0 --weight is deprecated in AMQP 0-9
                                        (fromIntegral $ length msg) --bodySize
                                        properties
                 ] ++
                 (if length msg > 0
                    then do
                      --split into frames of maxFrameSize
                      map ContentBodyPayload
                         (splitLen msg (fromIntegral $ connMaxFrameSize $ connection chan))
                    else [])
  writeFrames chan toWrite
      where
        splitLen str len | length str > len = (take len str):(splitLen (drop len str) len)
        splitLen str _ = [str]

writeAssembly' chan (SimpleMethod m) = do
    writeFrames chan [MethodPayload m]

-- most exported functions in this module will use either 'writeAssembly' or 'request' to talk to the server
-- so we perform the exception handling here

-- | writes an assembly to the channel
writeAssembly :: Channel -> Assembly -> IO ()
writeAssembly chan m =
    CE.catches
          (writeAssembly' chan m)
          [ CE.Handler (\ (_ :: AMQPException) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.ErrorCall) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.IOException) -> throwMostRelevantAMQPException chan)
          ]

-- this throws an AMQPException based on the status of the connection and the channel
-- if both connection and channel are closed, it will throw a ConnectionClosedException
throwMostRelevantAMQPException :: Channel -> IO b
throwMostRelevantAMQPException chan = do
  cc <- readMVar $ connClosed $ connection chan
  case cc of
    Just r -> CE.throwIO $ ConnectionClosedException r
    Nothing -> do
            chc <- readMVar $ chanClosed chan
            case chc of
              Just r -> CE.throwIO $ ChannelClosedException r
              Nothing -> CE.throwIO $ ConnectionClosedException "unknown reason"

-- | writes multiple frames to the channel atomically
writeFrames :: Channel -> [FramePayload] -> IO ()
writeFrames chan payloads =
    let conn = connection chan
    in withMVar (connChannels conn) $ \chans ->
        if member (fromIntegral $ channelID chan) chans
          then CE.catch
               -- ensure at most one thread is writing to the socket
               -- at any time
                   (withMVar (connWriteLock conn) $ \_ ->
                        mapM_ (\payload -> writeFrameSock (connSocket conn) (Frame (channelID chan) payload)) payloads)
                   ( \(_ :: CE.IOException) -> do
                       CE.throwIO $ userError "connection not open")
          else do
            CE.throwIO $ userError "channel not open"
