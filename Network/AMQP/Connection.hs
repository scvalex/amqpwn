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
        -- * Opaque connection type
        Connection,

        -- * Opening and closing connections
        openConnection,
        closeConnection, addConnectionClosedHandler
    ) where

import Control.Concurrent ( killThread, myThreadId, forkIO )
import Control.Concurrent.Chan ( writeChan )
import Control.Concurrent.MVar ( modifyMVar_, withMVar, newMVar
                               , newEmptyMVar, tryPutMVar, readMVar )
import qualified Control.Exception as CE
import Data.Binary ( Binary(..) )
import qualified Data.Binary.Put as Put
import Data.ByteString.Char8 ( pack )
import Data.ByteString.Lazy.Char8 ( unpack )
import qualified Data.IntMap as IM
import qualified Data.Map as Map
import Data.String ( fromString )
import Network.AMQP.Protocol ( readFrameSock, writeFrameSock )
import Network.AMQP.Helpers ( toStrict )
import Network.AMQP.Types ( Connection(..), Frame(..), FramePayload(..)
                          , ShortString(..), MethodPayload(..), ShortString
                          , Channel(..), FieldTable(..), LongString(..) )
import Network.BSD ( getProtocolNumber )
import Network.Socket ( Socket, socket, PortNumber, Family(..), inet_addr
                      , SocketType(..), connect, SockAddr(..), sClose )
import qualified Network.Socket.ByteString as NB
import Text.Printf ( printf )


data ConnectingState = CInitiating | CStarting1 | CStarting2 | CTuning
                     | COpening | COpen

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
                            modifyMVar_ (getConnClosed conn) $ \_ ->
                                return $ Just "closed by user"
                            killThread =<< myThreadId
          forwardToChannel 0 (MethodPayload (Connection_close _ s _ _ )) = do
                            let (ShortString errorMsg) = s
                            modifyMVar_ (getConnClosed conn) $ \_ ->
                                return $ Just errorMsg
                            killThread =<< myThreadId
          forwardToChannel 0 payload =
              printf "WARNING: unexpected msg on channel zero: %s"
                     (show payload)

          -- Forward asynchronous message to other channels
          forwardToChannel chanID payload =
              withMVar (getChannels conn) $ \cs ->
                  case IM.lookup (fromIntegral chanID) cs of
                    Just c -> writeChan (inQueue $ fst c) payload
                    Nothing -> printf "ERROR: channel %d not open" chanID

-- | Open a connection to an AMQP server.
--
-- NOTE: If the username, password or virtual host are invalid, this
-- method will throw a 'ConnectionClosedException'.  The exception
-- will not contain a reason why the connection was closed, so you'll
-- have to find out yourself.
--
-- FIXME: use a state machine for login
openConnection :: String          -- ^ hostname
               -> PortNumber     -- ^ port
               -> String         -- ^ virtual host
               -> String         -- ^ username
               -> String         -- ^ password
               -> IO Connection
openConnection host port vhost username password = do
  sock <- socket AF_INET Stream =<< getProtocolNumber "tcp"
  addr <- inet_addr host
  connect sock (SockAddrInet port addr)

  conn <- doConnectionOpen CInitiating sock 0

  -- spawn the connectionReceiver
  forkIO $ CE.finally (connectionReceiver conn) $ do
    -- try closing socket
    CE.catch (sClose sock) (\(_ :: CE.SomeException) -> return ())

    -- mark as closed
    modifyMVar_ (getConnClosed conn) $ \x -> return . Just $ maybe "closed" id x

    -- kill all channel-threads
    withMVar (getChannels conn) $ \cc -> mapM_ (\c -> killThread $ snd c) $
                                     IM.elems cc
    withMVar (getChannels conn) $ \_ -> return $ IM.empty

    -- mark connection as closed, so all pending calls to
    -- 'closeConnection' can now return
    tryPutMVar (getConnClosedLock conn) ()

    -- notify connection-close-handlers
    withMVar (getConnCloseHandlers conn) sequence

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
                    fail $ printf "unknown connection type: %s"
                                  (show methodPayload)
          Frame _ _ ->
              fail "unexpected frame on non-0 channel"
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
                -- FIXME add heartbeat support
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
                    fail $ printf "unhandled tune %s" (show methodPayload)
          Frame _ _ ->
              fail "unexpected frame on non-0 channel"
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
                    fail $ printf "unhandled open_ok %s" (show methodPayload)
          Frame _ _ ->
              fail "unexpected frame on non-0 channel"
      doConnectionOpen COpen sock frameMax = do
        -- Connection established!
        myConnChannels <- newMVar IM.empty
        cClosed <- newMVar Nothing
        ccl <- newEmptyMVar
        writeLock <- newMVar ()
        myConnClosedHandlers <- newMVar []
        lastChanId <- newMVar 0
        return $ Connection { getSocket = sock
                            , getChannels = myConnChannels
                            , getMaxFrameSize = frameMax
                            , getConnClosed = cClosed
                            , getConnClosedLock = ccl
                            , getConnWriteLock = writeLock
                            , getConnCloseHandlers = myConnClosedHandlers
                            , getLastChannelId = lastChanId
                            }

-- | Close a connection.
closeConnection :: Connection -> IO ()
closeConnection c = do
  CE.catch (
     withMVar (getConnWriteLock c) $ \_ ->
         writeFrameSock (getSocket c) $
                            (Frame 0 (MethodPayload (Connection_close
                                                     --TODO: set these values
                                                     0 -- reply_code
                                                     (ShortString "") -- reply_text
                                                     0 -- class_id
                                                     0 -- method_id
                                                    )))
           )
        (\ (_ :: CE.IOException) -> return ()) -- do nothing if
                                              -- connection is already
                                              -- closed

  -- wait for connection_close_ok by the server; this MVar gets filled
  -- in the CE.finally handler in openConnection
  readMVar $ getConnClosedLock c
  return ()

-- | Add a handler that will be called after the connection is closed
-- -- either by calling 'closeConnection' or by an exception.  If the
-- if-closed parameter is True and the connection is already closed,
-- the handler will be called immediately.  If if-closed is False and
-- the connection is already closed, the handler will never be called.
addConnectionClosedHandler :: Connection -- ^ the connection
                           -> Bool      -- ^ if-closed
                           -> IO ()      -- ^ handler
                           -> IO ()
addConnectionClosedHandler conn ifClosed handler = do
  withMVar (getConnClosed conn) $ \cc ->
      case cc of
        -- connection is already closed, so call the handler directly
        Just _ | ifClosed == True -> handler
        -- otherwise add it to the list
        _ -> modifyMVar_ (getConnCloseHandlers conn) $ \old ->
              return $ handler:old
