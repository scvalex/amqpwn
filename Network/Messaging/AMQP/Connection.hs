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

module Network.Messaging.AMQP.Connection (
        -- * Opening and closing connections
        openConnection, addConnectionClosedHandler,
        closeConnection, closeConnectionNormal,

        -- * Conection interal RPC, async
        request, request', async,

        -- * Channels (internal)
        openChannel, closeChannel
    ) where

import Control.Applicative ( (<$>) )
import Control.Concurrent ( killThread, myThreadId, forkIO )
import Control.Concurrent.STM ( atomically
                              , newTMVar, newEmptyTMVar, isEmptyTMVar
                              , readTMVar, tryPutTMVar, putTMVar, takeTMVar
                              , newTVar, readTVar, writeTVar )
import qualified Control.Exception as CE
import Control.Monad ( when )
import Data.Binary ( Binary(..) )
import qualified Data.Binary.Put as Put
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.IntMap as IM
import qualified Data.Map as M
import Data.String ( IsString(..) )
import Network.Messaging.Helpers ( toStrict, modifyTVar, withTMVarIO )
import Network.Messaging.AMQP.Types ( Connection(..), Channel(..)
                                    , Assembler(..), ChannelId
                                    , ChannelType(..), Frame(..)
                                    , FramePayload(..), Method(..)
                                    , MethodPayload(..), FieldTable(..)
                                    , ShortString(..), LongString(..)
                                    , AMQPException(..), getClassIdOf
                                    , readFrameSock, writeFrameSock
                                    , newEmptyAssembler )
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
        frame <- readFrameSock sock
        case frame of
          Frame 0 methodPayload ->
              case methodPayload of
                (MethodPayload (ConnectionStart 0 9 _ ms _))
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
        writeFrameSock sock . Frame 0 . MethodPayload $ ConnectionStartOk
                           (FieldTable (M.fromList []))
                           (ShortString "AMQPLAIN")
                           loginTable
                           (ShortString "en_US")
        doConnectionOpen CTuning sock frameMax
      doConnectionOpen CTuning sock frameMax = do
        -- S: tune
        frame <- readFrameSock sock
        case frame of
          (Frame 0 methodPayload) ->
              case methodPayload of
                (MethodPayload (ConnectionTune _ sFrameMax _)) -> do
                    let frameMax' =
                            if frameMax == 0
                              then (fromIntegral sFrameMax)
                              else min frameMax (fromIntegral sFrameMax)
                    -- C: tune_ok
                    writeFrameSock sock . Frame 0 . MethodPayload
                         $ ConnectionTuneOk 0 (fromIntegral frameMax') 0
                    doConnectionOpen COpening sock frameMax'
                _ ->
                    CE.throw . ConnectionStartException $
                      printf "unhandled tune %s" (show methodPayload)
          Frame _ _ ->
              CE.throw $ ConnectionStartException
                    "unexpected frame on non-0 channel"
      doConnectionOpen COpening sock frameMax = do
        -- C: open
        writeFrameSock sock . Frame 0 . MethodPayload  $ ConnectionOpen
                           (ShortString vhost) -- virtual host
                           (ShortString "")    -- capabilities
                           True                -- insist
        -- S: open_ok
        frame <- readFrameSock sock
        case frame of
          Frame 0 methodPayload ->
              case methodPayload of
                (MethodPayload (ConnectionOpenOk _)) ->
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
        return $ Connection { getSocket            = sockVar
                            , getMaxFrameSize      = fromIntegral frameMax
                            , getConnClosed        = cClosed
                            , getConnCloseHandlers = myConnClosedHandlers
                            , getChannels          = myConnChannels
                            , getLastChannelId     = lastChanId
                            }
      finalizeConnection conn reason = do
        -- try closing socket
        CE.catch (withTMVarIO (getSocket conn) sClose) $
                 \(_ :: CE.SomeException) ->
                     return ()

        handlers <- atomically $ do
          -- mark as closed
          tryPutTMVar (getConnClosed conn) reason

          -- kill any pending RPCs
          rpcs <- return . map (getChannelRPC . snd) . IM.toList
               =<< readTVar (getChannels conn)
          mapM_ (flip putTMVar (CE.throw reason)) rpcs

          -- clear channel threads
          writeTVar (getChannels conn) IM.empty

          readTVar (getConnCloseHandlers conn)

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
  -- Do nothing if connection is already closed.  Otherwise, wait for
  -- connection.close_ok by the server; this MVar gets filled in the
  -- CE.finally handler in openConnection.
  (doClose >> atomically (readTMVar (getConnClosed conn)) >> return ())
    `CE.catch` (\(_ :: CE.IOException) -> return ())
      where
        doClose = do
          withTMVarIO (getSocket conn) $ \sock ->
              writeFrameSock sock $ Frame 0 $
                             MethodPayload $ ConnectionClose
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
  (Frame chId payload) <- readFrameSock sock
  forwardToChannel (fromIntegral chId) payload
  connectionReceiver conn sock
      where
        -- Forward to channel0.
        forwardToChannel 0 (MethodPayload ConnectionCloseOk) = do
            atomically $ tryPutTMVar (getConnClosed conn)
                                     (ConnectionClosedException "Normal")
            killThread =<< myThreadId -- finalize connection will now run
        forwardToChannel 0 (MethodPayload (ConnectionClose _ s _ _ )) = do
            let (ShortString errorMsg) = s
            atomically $ tryPutTMVar (getConnClosed conn)
                                     (ConnectionClosedException errorMsg)
            killThread =<< myThreadId -- finalize connection will now run
        forwardToChannel 0 msg =
            CE.throw . ConnectionClosedException $
              printf "unexpected msg on channel zero: %s" (show msg)

        -- Ignore @channel.close_ok@.  Because we're awesome.
        forwardToChannel _ (MethodPayload ChannelCloseOk) =
            return ()
        -- See above.
        forwardToChannel _ (MethodPayload (ChannelOpenOk _)) =
            return ()

        -- Bump acks, nacks and returns to channel handlers
        forwardToChannel chId (MethodPayload p@(BasicAck _ _)) = do
            withChannelFail chId $ \ch -> do
              case getChannelType ch of
                (PublishingChannel _ handler _ _) ->
                    handler p
                _ ->
                    CE.throw $ ClientException "received ack on \
                                               \non-publishing channel"
        forwardToChannel chId (MethodPayload p@(BasicNack _ _ _)) = do
            withChannelFail chId $ \ch -> do
              case getChannelType ch of
                (PublishingChannel _ _ handler _) ->
                    handler p
                _ ->
                    CE.throw $ ClientException "received nack on \
                                               \non-publishing channel"

        -- Handle channel.close here
        forwardToChannel chId (MethodPayload (ChannelClose code reason _ _)) = do
            withChannel chId (return ()) $ \ch -> do
              let exc = ChannelClosedException $
                        printf "channel %d closed: %d '%s'"
                               chId code (show reason)
              case getChannelType ch of
                ControlChannel ->
                    atomically . putTMVar (getChannelRPC ch) $ CE.throw exc
                (PublishingChannel tid _ _ _) ->
                    CE.throwTo tid exc
            closeChannel conn chId

        -- Forward asynchronous message to other channels.
        forwardToChannel chId payload = do
            act <- atomically $ do
                     channels <- readTVar (getChannels conn)
                     case IM.lookup chId channels of
                       Just ch ->
                           return $ processChannelPayload ch payload
                       Nothing ->
                           return $ CE.throw . ConnectionClosedException $
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
              Nothing ->
                  return ()
              Just cm@(ContentMethod (BasicReturn _ _ _ _) _ _) ->
                  case getChannelType ch of
                    (PublishingChannel _ _ _ handler) ->
                        handler cm
                    _ ->
                        CE.throw $ ClientException "received return on \
                                                   \non-publishing channel"
              Just method ->
                  atomically $ putTMVar (getChannelRPC ch) method

        withChannelFail chId =
            withChannel chId (CE.throw . ClientException $
                              printf "received frame for non-existing \
                                     \channel: %d" chId)

        withChannel :: ChannelId -> IO a -> (Channel -> IO a) -> IO a
        withChannel chId failAct act = do
          mch <- atomically $ do
                  chs <- readTVar (getChannels conn)
                  case IM.lookup chId chs of
                    Nothing -> return Nothing
                    Just ch -> return (Just ch)
          case mch of
            Nothing   -> failAct
            (Just ch) -> act ch

-- | Perform an asynchronous AMQP request.
-- FIXME: Proper error handling.
async :: Connection -> ChannelId -> Method -> IO ()
async = unsafeWriteMethod

-- | Perform a synchroneous AMQP request.
request :: Connection -> Method -> IO Method
request conn method = do
  (chId, ch) <- openChannel conn ControlChannel
  request' conn ch chId method
    `CE.finally` closeChannel conn chId

-- FIXME: Proper error handling.
request' :: Connection -> Channel -> ChannelId -> Method -> IO Method
request' conn ch chId method = do
  unsafeWriteMethod conn chId method
  atomically $ takeTMVar (getChannelRPC ch)

-- | Write a method to the connection.  Does /not/ handle exceptions.
unsafeWriteMethod :: Connection -> ChannelId -> Method -> IO ()
unsafeWriteMethod conn chId (SimpleMethod m) =
    writeFrames conn chId [MethodPayload m]
unsafeWriteMethod conn chId (ContentMethod m chp content) =
    let contentFragements = [ MethodPayload m
                            , ContentHeaderPayload (getClassIdOf chp)
                                                   0
                                                   (fromIntegral $
                                                     BL.length content)
                                                   chp ] ++
                            fragment content
    in writeFrames conn chId contentFragements
        where
          fragment s
              | BL.null s = []
              | otherwise = let frameMax = getMaxFrameSize conn
                                (chunk, rest) = BL.splitAt (frameMax - 8) s
                            in (ContentBodyPayload chunk) : fragment rest

-- | Open a new channel and add it to the connection's channel map.
openChannel :: Connection -> ChannelType -> IO (ChannelId, Channel)
openChannel conn chType = do
  chId <- atomically $ modifyTVar (getLastChannelId conn) (+1)
  do
    ch0 <- newChannel
    let ch = ch0 { getChannelType = chType }
    atomically $ modifyTVar (getChannels conn) (\m -> IM.insert chId ch m)
    openChannel' conn chId
    return (chId, ch)
    `CE.catch` (\(e :: CE.IOException) -> do
                  atomically $ modifyTVar (getChannels conn)
                                          (\m -> IM.delete chId m)
                  CE.throw e)

-- | Perform the actual @channel.open@ asynchronously.  I.e. don't
-- wait for the @channel.open_ok@.
openChannel' :: Connection -> ChannelId -> IO ()
openChannel' conn chId =
    unsafeWriteMethod conn chId . SimpleMethod $ ChannelOpen (fromString "")

-- | Remove the channel from the connection's channel map and perform
-- the @channel.close@.  If the channel is unregistered, assume it's
-- already been closed.
closeChannel :: Connection -> ChannelId -> IO ()
closeChannel conn chId = do
  needsClosing <- atomically $ do
                    chs <- readTVar (getChannels conn)
                    case IM.lookup chId chs of
                      Nothing -> return False
                      Just _  -> do
                        modifyTVar (getChannels conn) (\m -> IM.delete chId m)
                        return True
  when needsClosing $ closeChannel' conn chId
  return ()

-- | Perform the actual @channel.close@ asynchronously.  I.e. don't
-- wait for the @channel.close_ok@.
closeChannel' :: Connection -> ChannelId -> IO ()
closeChannel' conn chId = do
    unsafeWriteMethod conn chId (SimpleMethod (ChannelClose 200 (fromString "Ok") 0 0))

-- | Create a new 'Channel' value.
newChannel :: IO Channel
newChannel = atomically $ do
  assembler <- newTVar newEmptyAssembler
  chanClosed <- newEmptyTMVar
  consumer <- newEmptyTMVar
  rpc <- newEmptyTMVar
  return Channel { getAssembler   = assembler
                 , getChanClosed  = chanClosed
                 , getConsumer    = consumer
                 , getChannelType = ControlChannel
                 , getChannelRPC  = rpc }

-- | Write the given frames to the connection/channel.
writeFrames :: Connection -> ChannelId -> [FramePayload] -> IO ()
writeFrames conn chId payloads = do
  withTMVarIO (getSocket conn) $ \sock -> do
      mapM_ (\p -> writeFrameSock sock (Frame (fromIntegral chId) p))
            payloads
      `CE.catch`
        (\(e :: CE.IOException) -> CE.throw . ClientException $
                                 printf "IOException on %d: %s" chId (show e))
