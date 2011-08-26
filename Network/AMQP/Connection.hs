{-# LANGUAGE ScopedTypeVariables #-}

-- | A connection is a thread that data from the socket, assembles
-- complete 'Method's and processes them, either by updating its
-- internal state or by forwarding it to the user application.
--
-- Incoming Data: Socket -> Connection Thread -> Application
--
-- Commands issued by the application are stored in a queue and
-- executed sequentially.  So, as a rule, all functions that work on
-- 'Connection's are thread-safe.
--
-- Outgoing Data: Application -> Command Queue -> Socket

module Network.AMQP.Connection (
        -- * Opening and closing connections
        openConnection, addConnectionClosedHandler,
        closeConnection, closeConnectionNormal,

        -- * Conection interal RPC
        request
    ) where

import Control.Applicative ( (<$>) )
import Control.Concurrent ( killThread, myThreadId, forkIO )
import Control.Concurrent.STM ( atomically
                              , newTMVar, newEmptyTMVar, isEmptyTMVar
                              , readTMVar, tryPutTMVar, putTMVar, takeTMVar
                              , newTVar, readTVar, writeTVar
                              , newTChan, isEmptyTChan, readTChan, writeTChan )
import qualified Control.Exception as CE
import Data.Binary ( Binary(..) )
import qualified Data.Binary.Put as Put
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.IntMap as IM
import qualified Data.Map as M
import Data.String ( IsString(..) )
import Network.AMQP.Protocol ( readFrameSock, writeFrameSock, writeFrames
                             , newEmptyAssembler )
import Network.AMQP.Helpers ( toStrict, modifyTVar, withTMVarIO )
import Network.AMQP.Types ( Connection(..), Channel(..), Assembler(..)
                          , ChannelId, controlChannel
                          , Frame(..), FramePayload(..)
                          , Method(..), MethodPayload(..)
                          , FieldTable(..), ShortString(..), LongString(..)
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
  forkIO $ (connectionReceiver conn sock)
           `CE.catch` (\(e :: AMQPException) -> finalizeConnection conn e)
           `CE.finally` (finalizeConnection conn $
                         ConnectionClosedException "Normal")
  openChannel conn controlChannel
  return conn
    where
      doConnectionOpen :: ConnectingState -> Socket -> Int -> IO Connection
      doConnectionOpen CInitiating sock frameMax = do
        -- C: protocol-header
        NB.send sock . toStrict . Put.runPut $ do
          Put.putByteString $ BS.pack "AMQP"
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
                    CE.throw . ConnectionStartException $
                                 printf "unknown connection type: %s"
                                        (show methodPayload)
          Frame _ _ ->
              CE.throw $ ConnectionStartException
                    "unexpected frame on non-0 channel"
      doConnectionOpen CStarting2 sock frameMax = do
        -- C: start_ok
        let loginTable = LongString . drop 4 . BL.unpack . Put.runPut . put $
               FieldTable (M.fromList [ ( fromString "LOGIN"
                                        , fromString username)
                                      , ( fromString "PASSWORD"
                                        , fromString password)
                                      ])
        writeFrameSock sock . Frame 0 . MethodPayload $ Connection_start_ok
                           (FieldTable (M.fromList []))
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
        sockVar <- newTMVar sock
        cClosed <- newEmptyTMVar
        myConnClosedHandlers <- newTVar []
        myConnChannels <- newTVar IM.empty
        lastChanId <- newTVar 0
        rpcQueue <- newTChan
        return $ Connection { getSocket            = sockVar
                            , getMaxFrameSize      = frameMax
                            , getConnClosed        = cClosed
                            , getConnCloseHandlers = myConnClosedHandlers
                            , getChannels          = myConnChannels
                            , getLastChannelId     = lastChanId
                            , getRPCQueue          = rpcQueue
                            }
      finalizeConnection conn reason = do
        -- try closing socket
        CE.catch (withTMVarIO (getSocket conn) sClose) $
                 \(_ :: CE.SomeException) ->
                     return ()

        handlers <- atomically $ do
          -- mark as closed
          tryPutTMVar (getConnClosed conn) reason

          -- clear channel threads
          writeTVar (getChannels conn) IM.empty

          -- kill any pending RPCs
          killRPCQueue reason (getRPCQueue conn)

          readTVar (getConnCloseHandlers conn)

        -- notify connection-close-handlers
        mapM_ ($ reason) handlers

      killRPCQueue reason queue = do
        emp <- isEmptyTChan queue
        if emp
          then return ()
          else do
            x <- readTChan queue
            tryPutTMVar x $ CE.throw reason
            killRPCQueue reason queue

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
  atomically $ readTMVar (getConnClosed conn)
  return ()
    where
      doClose = do
        withTMVarIO (getSocket conn) $ \sock ->
            writeFrameSock sock $ Frame 0 $
                           MethodPayload $ Connection_close
                                             (fromIntegral replyCode)
                                             (fromString replyText)
                                             0 -- class_id
                                             0 -- method_id

-- | Add a handler that will be called after the connection is closed
-- either by calling 'closeConnection' or by an exception.  If the
-- connection is already closed, the handler is called immediately in
-- the current thread.
addConnectionClosedHandler :: Connection                -- ^ the connection
                           -> (AMQPException -> IO ()) -- ^ handler
                           -> IO ()
addConnectionClosedHandler conn handler = do
  closeReason <- atomically $ do
                    notConnClosed <- isEmptyTMVar (getConnClosed conn)
                    if notConnClosed
                      then do
                        modifyTVar (getConnCloseHandlers conn) $ \handlers ->
                            handler : handlers
                        return Nothing
                      else do
                        Just <$> readTMVar (getConnClosed conn)
  case closeReason of
    Nothing -> return ()
    Just r  -> handler r

-- | Process: reads incoming frames from socket and forwards them to
-- opened channels.
connectionReceiver :: Connection -> Socket -> IO ()
connectionReceiver conn sock = do
    (Frame chId payload) <- readFrameSock sock (getMaxFrameSize conn)
    printf "Inbound frame: %s\n" (show payload)
    forwardToChannel chId payload
    connectionReceiver conn sock
        where
          -- Forward to channel0
          forwardToChannel 0 (MethodPayload Connection_close_ok) = do
              atomically $ tryPutTMVar (getConnClosed conn)
                                       (ConnectionClosedException "Normal")
              killThread =<< myThreadId -- finalize connection will now run
          forwardToChannel 0 (MethodPayload (Connection_close _ s _ _ )) = do
              let (ShortString errorMsg) = s
              atomically $ tryPutTMVar (getConnClosed conn)
                                       (ConnectionClosedException errorMsg)
              killThread =<< myThreadId -- finalize connection will now run
          forwardToChannel 0 msg =
              CE.throw . ConnectionClosedException $
                printf "unexpected msg on channel zero: %s" (show msg)

          -- Forward asynchronous message to other channels
          forwardToChannel chId (MethodPayload (Channel_close _ _ _ _))
             | fromIntegral chId == controlChannel = do
              atomically $ do
                resp <- readTChan (getRPCQueue conn)
                putTMVar resp . CE.throw . ChannelClosedException $
                           printf "channel %d closed" chId
              unsafeWriteMethod conn controlChannel (SimpleMethod Channel_close_ok)
              forkIO $ openChannel' conn controlChannel
              return ()
          forwardToChannel chId payload = do
              act <- atomically $ do
                       channels <- readTVar (getChannels conn)
                       case IM.lookup (fromIntegral chId) channels of
                         Just ch -> return $ processChannelPayload ch payload
                         Nothing -> return $ CE.throw . ConnectionClosedException $
                                      printf "channel %d not open" chId
              act

          processChannelPayload ch payload = do
              mMethod <- atomically $ do
                                 (Assembler assembler) <- readTVar (getAssembler ch)
                                 case assembler payload of
                                   Left assembler' -> do
                                       writeTVar (getAssembler ch) assembler'
                                       return Nothing
                                   Right (method, assembler') -> do
                                       writeTVar (getAssembler ch) assembler'
                                       return (Just method)
              case mMethod of
                Nothing     -> return ()
                Just method -> handleInboundMethod method

          handleInboundMethod method = do
            atomically $ do
              noRPC <- isEmptyTChan (getRPCQueue conn)
              resp <- readTChan (getRPCQueue conn)
              putTMVar resp $ if not noRPC
                              then method
                              else CE.throw . ClientException $
                                   printf "got reponse but no pending RPC: %s"
                                          (show method)
              -- I feel dirty now.

-- | Perform a synchroneous AMQP requst.
request :: Connection -> Method -> IO Method
request conn method = request' conn controlChannel method

-- FIXME: There's a race between writing res to the rpc queue and
-- writing the command on the wire.
-- FIXME: Proper error handling.
request' :: Connection -> ChannelId -> Method -> IO Method
request' conn chId method = do
  res <- atomically $ newEmptyTMVar
  atomically $ writeTChan (getRPCQueue conn) res
  unsafeWriteMethod conn chId method
  atomically $ takeTMVar res

-- | Write a method to the connection.  Does /not/ handle exceptions.
unsafeWriteMethod :: Connection -> ChannelId -> Method -> IO ()
unsafeWriteMethod conn chId (SimpleMethod m) = do
    writeFrames conn chId [MethodPayload m]

-- | Open a new channel and add it to the connection's channel map.
openChannel :: Connection -> ChannelId -> IO ()
openChannel conn chId = do
  ch <- newChannel
  atomically $ modifyTVar (getChannels conn) (\m -> IM.insert chId ch m)
  openChannel' conn chId
  `CE.catch` (\(e :: CE.IOException) -> do
                atomically $ modifyTVar (getChannels conn)
                                        (\m -> IM.delete chId m)
                CE.throw e)

-- | Perform the actual @channel.open@.
openChannel' :: Connection -> ChannelId -> IO ()
openChannel' conn chId = do
  resp <- request' conn chId . SimpleMethod $ Channel_open (fromString "")
  let (SimpleMethod (Channel_open_ok _)) = resp
  return ()

-- | Create a new 'Channel' value.
newChannel :: IO Channel
newChannel = atomically $ do
  assembler <- newTVar newEmptyAssembler
  chanClosed <- newEmptyTMVar
  consumer <- newEmptyTMVar
  return Channel { getAssembler  = assembler
                 , getChanClosed = chanClosed
                 , getConsumer   = consumer }
