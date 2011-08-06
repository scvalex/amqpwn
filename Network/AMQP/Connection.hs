{-# LANGUAGE ScopedTypeVariables #-}

-- | General concept: each connection has its own thread; each channel
-- has its own thread.  The connection reads data from the socket and
-- forwards it to the channel.  The channel processes data and
-- forwards it to the application.  Outgoing data is written directly
-- onto the socket.
--
-- Incoming Data: Socket -> Connection-Thread -> Channel-Thread -> Application
-- Outgoing Data: Application -> Socket

module Network.AMQP.Connection (
        -- * Opening and closing connections
        openConnection, addConnectionClosedHandler,
        closeConnection, closeConnectionNormal
    ) where

import Control.Applicative ( (<$>) )
import Control.Concurrent ( killThread, myThreadId, forkIO )
import Control.Concurrent.STM ( atomically, newTMVar, newEmptyTMVar
                              , takeTMVar, putTMVar, tryPutTMVar, readTMVar
                              , writeTChan, isEmptyTMVar )
import qualified Control.Exception as CE
import Data.Binary ( Binary(..) )
import qualified Data.Binary.Put as Put
import Data.ByteString.Char8 ( pack )
import Data.ByteString.Lazy.Char8 ( unpack )
import qualified Data.IntMap as IM
import qualified Data.Map as Map
import Data.String ( IsString(..) )
import Network.AMQP.Protocol ( readFrameSock, writeFrameSock )
import Network.AMQP.Helpers ( toStrict )
import Network.AMQP.Types ( Connection(..), Frame(..), FramePayload(..)
                          , ShortString(..), MethodPayload(..), ShortString
                          , Channel(..), FieldTable(..), LongString(..)
                          , AMQPException(..) )
import Network.BSD ( getProtocolNumber, getHostByName, hostAddress )
import Network.Socket ( Socket, socket, Family(..), SocketType(..), connect
                      , SockAddr(..), sClose )
import qualified Network.Socket.ByteString as NB
import Text.Printf ( printf )


data ConnectingState = CInitiating | CStarting1 | CStarting2 | CTuning
                     | COpening | COpen


-- | Open a connection to an AMQP server.
--
-- NOTE: If the username, password or virtual host are invalid, this
-- method will throw a 'ConnectionClosedException'.  The exception
-- will not contain a reason why the connection was closed, so you'll
-- have to find out yourself.
--
-- May throw 'ConnectionStartException' if the server handshake fails.
openConnection :: (Integral n)
               => String         -- ^ hostname
               -> n              -- ^ port
               -> String         -- ^ virtual host
               -> String         -- ^ username
               -> String         -- ^ password
               -> IO Connection
openConnection host port vhost username password = do
  sock <- socket AF_INET Stream =<< getProtocolNumber "tcp"
  addr <- hostAddress <$> getHostByName host
  connect sock (SockAddrInet (fromIntegral port) addr)

  conn <- doConnectionOpen CInitiating sock 0
  -- spawn the connectionReceiver
  forkIO $ (connectionReceiver conn)
           `CE.catch` (\(e :: AMQPException) -> finalizeConnection conn e)
           `CE.finally` (finalizeConnection conn $
                         ConnectionClosedException "Normal")
  return conn
    where
      doConnectionOpen :: ConnectingState -> Socket -> Int -> IO Connection
      doConnectionOpen CInitiating sock frameMax = do
        -- C: protocol-header
        NB.send sock . toStrict . Put.runPut $ do
          Put.putByteString $ pack "AMQP"
          mapM_ Put.putWord8 [0, 0, 9, 1]
        doConnectionOpen CStarting1 sock frameMax
      doConnectionOpen CStarting1 sock frameMax = do
        -- S: connection.start
        frame <- readFrameSock sock 4096
        case frame of
          Frame 0 methodPayload ->
              case methodPayload of
                (MethodPayload (Connection_start 0 9 _ ms _))
                                 | "AMQPLAIN" `elem` (words $ show ms) ->
                    doConnectionOpen CStarting2 sock frameMax
                _ ->
                    -- FIXME proper errors
                    CE.throw . ConnectionStartException $
                                 printf "unknown connection type: %s"
                                        (show methodPayload)
          Frame _ _ ->
              CE.throw $ ConnectionStartException
                    "unexpected frame on non-0 channel"
      doConnectionOpen CStarting2 sock frameMax = do
        -- C: start_ok
        let loginTable = LongString . drop 4 . unpack . Put.runPut . put $
               FieldTable (Map.fromList [ ( fromString "LOGIN"
                                          , fromString username)
                                        , ( fromString "PASSWORD"
                                          , fromString password)
                                        ])
        writeFrameSock sock . Frame 0 . MethodPayload $ Connection_start_ok
                           (FieldTable (Map.fromList []))
                           (ShortString "AMQPLAIN")
                           loginTable
                           (ShortString "en_US")
        doConnectionOpen CTuning sock frameMax
      doConnectionOpen CTuning sock frameMax = do
        -- S: tune
        frame <- readFrameSock sock 4096
        case frame of
          (Frame 0 methodPayload) ->
              case methodPayload of
                (MethodPayload (Connection_tune _ sFrameMax _)) -> do
                    let frameMax' =
                            if frameMax == 0
                              then (fromIntegral sFrameMax)
                              else min frameMax (fromIntegral sFrameMax)
                    -- C: tune_ok
                    writeFrameSock sock . Frame 0 . MethodPayload
                         $ Connection_tune_ok 0 (fromIntegral frameMax') 0
                    doConnectionOpen COpening sock frameMax'
                _ ->
                    CE.throw . ConnectionStartException $
                      printf "unhandled tune %s" (show methodPayload)
          Frame _ _ ->
              CE.throw $ ConnectionStartException
                    "unexpected frame on non-0 channel"
      doConnectionOpen COpening sock frameMax = do
        -- C: open
        writeFrameSock sock . Frame 0 . MethodPayload  $ Connection_open
                           (ShortString vhost) -- virtual host
                           (ShortString "")    -- capabilities
                           True                -- insist
        -- S: open_ok
        frame <- readFrameSock sock frameMax
        case frame of
          Frame 0 methodPayload ->
              case methodPayload of
                (MethodPayload (Connection_open_ok _)) ->
                    doConnectionOpen COpen sock frameMax
                _ ->
                    CE.throw . ConnectionStartException $
                      printf "unhandled open_ok %s" (show methodPayload)
          Frame _ _ ->
              CE.throw $ ConnectionStartException
                    "unexpected frame on non-0 channel"
      doConnectionOpen COpen sock frameMax = atomically $ do
        -- Connection established!
        myConnChannels <- newTMVar IM.empty
        cClosed <- newEmptyTMVar
        ccl <- newEmptyTMVar
        writeLock <- newTMVar ()
        myConnClosedHandlers <- newTMVar []
        lastChanId <- newTMVar 0
        return $ Connection { getSocket = sock
                            , getChannels = myConnChannels
                            , getMaxFrameSize = frameMax
                            , getConnClosed = cClosed
                            , getConnClosedLock = ccl
                            , getConnWriteLock = writeLock
                            , getConnCloseHandlers = myConnClosedHandlers
                            , getLastChannelId = lastChanId
                            }
      finalizeConnection conn reason = do
        -- try closing socket
        CE.catch (sClose $ getSocket conn) $ \(_ :: CE.SomeException) ->
            return ()

        (channels, handlers) <- atomically $ do
          -- mark as closed
          tryPutTMVar (getConnClosed conn) reason

          -- clear channel threads
          channels <- takeTMVar (getChannels conn)
          putTMVar (getChannels conn) IM.empty

          -- mark connection as closed, so all pending calls to
          -- 'closeConnection' can now return
          tryPutTMVar (getConnClosedLock conn) ()

          handlers <- takeTMVar (getConnCloseHandlers conn)

          return (channels, handlers)

        -- kill channels threads
        mapM_ (\c -> killThread $ snd c) (IM.elems channels)
        -- notify connection-close-handlers
        mapM_ ($ reason) handlers

-- | Close a connection normally.
closeConnectionNormal :: Connection -> IO ()
closeConnectionNormal = closeConnection (200 :: Int) "Goodbye"

-- | Close a connection.
closeConnection :: (Integral n1)
                => n1            -- ^ reply code
                -> String        -- ^ reply text
                -> Connection    -- ^ connection to close
                -> IO ()
closeConnection replyCode replyText conn = do
  -- do nothing if connection is already closed
  CE.catch doClose $ \ (_ :: CE.IOException) ->
      return ()
  -- wait for connection_close_ok by the server; this MVar gets filled
  -- in the CE.finally handler in openConnection
  atomically $ readTMVar (getConnClosedLock conn)
  return ()
    where
      doClose = do
        atomically $ takeTMVar (getConnWriteLock conn)
        writeFrameSock (getSocket conn) $ Frame 0 $
                MethodPayload $ Connection_close
                                  (fromIntegral replyCode)
                                  (fromString replyText)
                                  0 -- class_id
                                  0 -- method_id
        atomically $ putTMVar (getConnWriteLock conn) ()

-- | Add a handler that will be called after the connection is closed
-- either by calling 'closeConnection' or by an exception.  If the
-- connection is already closed, the handler is called immediately in
-- the current thread.
addConnectionClosedHandler :: Connection                -- ^ the connection
                           -> (AMQPException -> IO ()) -- ^ handler
                           -> IO ()
addConnectionClosedHandler conn handler = do
  notConnClosed <- atomically $ isEmptyTMVar (getConnClosed conn)
  if notConnClosed
     then atomically $ do
       handlers <- takeTMVar (getConnCloseHandlers conn)
       putTMVar (getConnCloseHandlers conn) (handler:handlers)
     else handler =<< atomically (readTMVar $ getConnClosed conn)

-- | Process: reads incoming frames from socket and forwards them to
-- opened channels.
connectionReceiver :: Connection -> IO ()
connectionReceiver conn = do
    (Frame chanID payload) <- readFrameSock (getSocket conn)
                                            (getMaxFrameSize conn)
    forwardToChannel chanID payload
    connectionReceiver conn
        where
          -- Forward to channel0
          forwardToChannel 0 (MethodPayload Connection_close_ok) = do
              atomically $ tryPutTMVar (getConnClosed conn)
                                       (ConnectionClosedException "Normal")
              killThread =<< myThreadId
          forwardToChannel 0 (MethodPayload (Connection_close _ s _ _ )) = do
              let (ShortString errorMsg) = s
              atomically $ tryPutTMVar (getConnClosed conn)
                                       (ConnectionClosedException errorMsg)
              killThread =<< myThreadId
          forwardToChannel 0 msg =
              CE.throw . ConnectionClosedException $
                printf "unexpected msg on channel zero: %s" (show msg)

          -- Forward asynchronous message to other channels
          forwardToChannel chanId payload = do
              channels <- atomically $ readTMVar (getChannels conn)
              case IM.lookup (fromIntegral chanId) channels of
                Just ch -> atomically $ writeTChan (getInQueue $ fst ch)
                                                   payload
                Nothing -> CE.throw . ConnectionClosedException $
                                printf "channel %d not open" chanId
