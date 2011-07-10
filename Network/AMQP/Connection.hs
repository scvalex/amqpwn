{-# LANGUAGE ScopedTypeVariables #-}

module Network.AMQP.Connection (
        Connection,
        openConnection, openConnection',
        closeConnection, addConnectionClosedHandler
    ) where

import Prelude hiding ( lookup )

import Control.Concurrent ( killThread, myThreadId, forkIO )
import Control.Concurrent.Chan ( writeChan )
import Control.Concurrent.MVar ( modifyMVar_, withMVar, newMVar
                               , newEmptyMVar, tryPutMVar, readMVar )
import qualified Control.Exception as CE
import Data.Binary ( Binary(..) )
import qualified Data.Binary.Put as Put
import Data.ByteString.Char8 ( pack )
import Data.ByteString.Lazy.Char8 ( unpack )
import Data.IntMap ( lookup, empty, elems )
import qualified Data.Map as Map
import Network.AMQP.Protocol ( readFrameSock, writeFrameSock )
import Network.AMQP.Helpers ( toStrict )
import Network.AMQP.Types ( Connection(..), Frame(..), FramePayload(..)
                          , ShortString(..), MethodPayload(..), ShortString
                          , Channel(..), FieldTable(..), LongString(..)
                          , FieldValue(..) )
import Network.BSD ( getProtocolNumber )
import Network.Socket ( socket, PortNumber, Family(..), inet_addr
                      , SocketType(..), connect, SockAddr(..), sClose )
import qualified Network.Socket.ByteString as NB

------------ CONNECTION -------------------

{- general concept: Each connection has its own thread. Each channel
has its own thread.  Connection reads data from socket and forwards it
to channel. Channel processes data and forwards it to application.
Outgoing data is written directly onto the socket.

Incoming Data: Socket -> Connection-Thread -> Channel-Thread -> Application
Outgoing Data: Application -> Socket
-}

-- | reads incoming frames from socket and forwards them to the opened channels
connectionReceiver :: Connection -> IO ()
connectionReceiver conn = do
    (Frame chanID payload) <- readFrameSock (connSocket conn) (connMaxFrameSize conn)
    forwardToChannel chanID payload
    connectionReceiver conn
  where

    forwardToChannel 0 (MethodPayload Connection_close_ok) = do
        modifyMVar_ (connClosed conn) $ \_ -> return $ Just "closed by user"
        killThread =<< myThreadId


    forwardToChannel 0 (MethodPayload (Connection_close _ (ShortString errorMsg) _ _ )) = do
        modifyMVar_ (connClosed conn) $ \_ -> return $ Just errorMsg
 
        killThread =<< myThreadId
    
    forwardToChannel 0 payload = print $ "Got unexpected msg on channel zero: "++(show payload)
   
    forwardToChannel chanID payload = do 
        --got asynchronous msg => forward to registered channel
        withMVar (connChannels conn) $ \cs -> do
            case lookup (fromIntegral chanID) cs of
                Just c -> writeChan (inQueue $ fst c) payload
                Nothing -> print $ "ERROR: channel not open "++(show chanID)

-- | @openConnection hostname virtualHost loginName loginPassword@
-- opens a connection to an AMQP server running on @hostname@.
-- @virtualHost@ is used as a namespace for AMQP resources (default is
-- \"/\"), so different applications could use multiple virtual hosts
-- on the same AMQP server
--
-- NOTE: If the login name, password or virtual host are invalid, this
-- method will throw a 'ConnectionClosedException'. The exception will
-- not contain a reason why the connection was closed, so you'll have
-- to find out yourself.
openConnection :: String -> String -> String -> String -> IO Connection
openConnection host vhost loginName loginPassword =
    openConnection' host 5672 vhost loginName loginPassword

-- | same as 'openConnection' but allows you to specify a non-default
-- port-number as the 2nd parameter
openConnection' :: String -> PortNumber -> String -> String -> String
                -> IO Connection
openConnection' host port vhost loginName loginPassword = do
  proto <- getProtocolNumber "tcp"
  sock <- socket AF_INET Stream proto
  addr <- inet_addr host
  connect sock (SockAddrInet port addr)
  NB.send sock $ toStrict $ Put.runPut $ do
    Put.putByteString $ pack "AMQP"
    Put.putWord8 1
    Put.putWord8 1 --TCP/IP
    Put.putWord8 9 --Major Version
    Put.putWord8 1 --Minor Version

  -- S: connection.start
  Frame 0 (MethodPayload (Connection_start _ _ _ _ _)) <- readFrameSock sock 4096

  -- C: start_ok
  writeFrameSock sock start_ok
  -- S: tune
  Frame 0 (MethodPayload (Connection_tune _ frame_max _)) <- readFrameSock sock 4096
  -- C: tune_ok
  let maxFrameSize = (min 131072 frame_max)

  writeFrameSock sock (Frame 0 (MethodPayload (Connection_tune_ok 0 maxFrameSize 0)))
  -- C: open
  writeFrameSock sock open

  -- S: open_ok
  Frame 0 (MethodPayload (Connection_open_ok _)) <-
      readFrameSock sock $ fromIntegral maxFrameSize

  -- Connection established!

  --build Connection object
  myConnChannels <- newMVar empty
  lastChanID <- newMVar 0
  cClosed <- newMVar Nothing
  writeLock <- newMVar ()
  ccl <- newEmptyMVar
  myConnClosedHandlers <- newMVar []
  let conn = Connection sock myConnChannels (fromIntegral maxFrameSize)
                        cClosed ccl writeLock myConnClosedHandlers lastChanID

  --spawn the connectionReceiver
  forkIO $ CE.finally (connectionReceiver conn) $ do
    -- try closing socket
    CE.catch (sClose sock) (\(_::CE.SomeException) -> return ())

    -- mark as closed
    modifyMVar_ cClosed $ \x -> return $ Just $ maybe "closed" id x

    --kill all channel-threads
    withMVar myConnChannels $ \cc -> mapM_ (\c -> killThread $ snd c) $
                                     elems cc
    withMVar myConnChannels $ \_ -> return $ empty

    -- mark connection as closed, so all pending calls to
    -- 'closeConnection' can now return
    tryPutMVar ccl ()

    -- notify connection-close-handlers
    withMVar myConnClosedHandlers sequence

  return conn
    where
      start_ok = (Frame 0 (MethodPayload (Connection_start_ok  (FieldTable (Map.fromList []))
                                          (ShortString "AMQPLAIN")
        --login has to be a table without first 4 bytes
        (LongString (drop 4 $ unpack $ Put.runPut $ put $ FieldTable (Map.fromList [(ShortString "LOGIN",FVLongString $ LongString loginName), (ShortString "PASSWORD", FVLongString $ LongString loginPassword)])))
        (ShortString "en_US")) ))
      open = (Frame 0 (MethodPayload (Connection_open
        (ShortString vhost)  --virtual host
        (ShortString "")   -- capabilities
        True)))   --insist; True because we don't support redirect yet

-- | closes a connection
closeConnection :: Connection -> IO ()
closeConnection c = do
  CE.catch (
     withMVar (connWriteLock c) $ \_ -> writeFrameSock (connSocket c) $ (Frame 0 (MethodPayload (Connection_close
            --TODO: set these values
            0 -- reply_code
            (ShortString "") -- reply_text
            0 -- class_id
            0 -- method_id
            )))
            )
        (\ (_::CE.IOException) -> do
            --do nothing if connection is already closed
            return ()
        )

  -- wait for connection_close_ok by the server; this MVar gets filled in the CE.finally handler in openConnection'
  readMVar $ connClosedLock c
  return ()

-- | @addConnectionClosedHandler conn ifClosed handler@ adds a
-- @handler@ that will be called after the connection is closed
-- (either by calling @closeConnection@ or by an exception). If the
-- @ifClosed@ parameter is True and the connection is already closed,
-- the handler will be called immediately. If @ifClosed == False@ and
-- the connection is already closed, the handler will never be called
addConnectionClosedHandler :: Connection -> Bool -> IO () -> IO ()
addConnectionClosedHandler conn ifClosed handler = do
  withMVar (connClosed conn) $ \cc -> do
    case cc of
      -- connection is already closed, so call the handler directly
      Just _ | ifClosed == True -> handler
      -- otherwise add it to the list
      _ -> modifyMVar_ (connClosedHandlers conn) $ \old -> return $ handler:old
