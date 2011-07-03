{-# OPTIONS -XBangPatterns -XScopedTypeVariables -XDeriveDataTypeable #-}
{- |

A client library for AMQP servers implementing the 0-8 spec; currently only supports RabbitMQ (see <http://www.rabbitmq.com>)

A good introduction to AMQP can be found here (though it uses Python): <http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/>

/Example/:

Connect to a server, declare a queue and an exchange and setup a callback for messages coming in on the queue. Then publish a single message to our new exchange

>import Network.AMQP
>import qualified Data.ByteString.Lazy.Char8 as BL
>
>main = do
>    conn <- openConnection "127.0.0.1" "/" "guest" "guest"
>    chan <- openChannel conn
>    
>    -- declare a queue, exchange and binding
>    declareQueue chan newQueue {queueName = "myQueue"}
>    declareExchange chan newExchange {exchangeName = "myExchange", exchangeType = "direct"}
>    bindQueue chan "myQueue" "myExchange" "myKey"
>
>    -- subscribe to the queue
>    consumeMsgs chan "myQueue" Ack myCallback
>
>    -- publish a message to our new exchange
>    publishMsg chan "myExchange" "myKey" 
>        newMsg {msgBody = (BL.pack "hello world"), 
>                msgDeliveryMode = Just Persistent}
>
>    getLine -- wait for keypress
>    closeConnection conn
>    putStrLn "connection closed"
>
>    
>myCallback :: (Message,Envelope) -> IO ()
>myCallback (msg, env) = do
>    putStrLn $ "received message: "++(BL.unpack $ msgBody msg)
>    -- acknowledge receiving the message
>    ackEnv env

/Exception handling/:

Some function calls can make the AMQP server throw an AMQP exception, which has the side-effect of closing the connection or channel. The AMQP exceptions are raised as Haskell exceptions (see 'AMQPException'). So upon receiving an 'AMQPException' you may have to reopen the channel or connection.

-}
module Network.AMQP (

    -- * Connection
    Connection,
    openConnection,
    openConnection',
    closeConnection,
    addConnectionClosedHandler,
   
    -- * Channel
    Channel,
    openChannel,
    
    -- * Exchanges
    ExchangeOpts(..),
    newExchange,
    declareExchange,
    deleteExchange,
    
    -- * Queues
    QueueOpts(..),
    newQueue,
    declareQueue,
    bindQueue,
    purgeQueue,
    deleteQueue,
    
    
    -- * Messaging
    Message(..),
    DeliveryMode(..),
    newMsg,
    Envelope(..),
    ConsumerTag,
    Ack(..),
    consumeMsgs,
    cancelConsumer,
    publishMsg,
    getMsg,
    rejectMsg,
    recoverMsgs,
    
    ackMsg,
    ackEnv,
    
    -- * Transactions
    txSelect,
    txCommit,
    txRollback,
    
    -- * Flow Control
    flow,
    
    
    -- * Exceptions
    AMQPException(..)
    
    
    
    
 
) where



import Data.Binary
import Data.Binary.Get
import Data.Binary.Put as BPut
import Data.Typeable
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Maybe

import Control.Concurrent
import Control.Monad
import qualified Control.Exception as CE

import Network.BSD
import Network.Socket
import qualified Network.Socket.ByteString as NB

import Network.AMQP.Protocol
import Network.AMQP.Types
import Network.AMQP.Helpers
import Network.AMQP.Framing



{-
TODO:
- basic.qos
- handle basic.return
- connection.secure
- connection.redirect
-}



----- EXCHANGE -----


-- | A record that contains the fields needed when creating a new exhange using 'declareExchange'. The default values apply when you use 'newExchange'.
data ExchangeOpts = ExchangeOpts 
                {
                    exchangeName :: String, -- ^ (must be set); the name of the exchange
                    exchangeType :: String, -- ^ (must be set); the type of the exchange (\"fanout\", \"direct\", \"topic\")
                    
                    -- optional
                    exchangePassive :: Bool, -- ^ (default 'False'); If set, the server will not create the exchange. The client can use this to check whether an exchange exists without modifying the server state.
                    exchangeDurable :: Bool, -- ^ (default 'True'); If set when creating a new exchange, the exchange will be marked as durable. Durable exchanges remain active when a server restarts. Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
                    exchangeAutoDelete :: Bool, -- ^ (default 'False'); If set, the exchange is deleted when all queues have finished using it.
                    exchangeInternal :: Bool -- ^ (default 'False'); If set, the exchange may not be used directly by publishers, but only when bound to other exchanges. Internal exchanges are used to construct wiring that is not visible to applications.
                }

-- | an 'ExchangeOpts' with defaults set; you must override at least the 'exchangeName' and 'exchangeType' fields. 
newExchange :: ExchangeOpts                
newExchange = ExchangeOpts "" "" False True False False

-- | declares a new exchange on the AMQP server. Can be used like this: @declareExchange channel newExchange {exchangeName = \"myExchange\", exchangeType = \"fanout\"}@
declareExchange :: Channel -> ExchangeOpts -> IO ()
declareExchange chan exchg = do
    (SimpleMethod Exchange_declare_ok) <- request chan (SimpleMethod (Exchange_declare
        1 -- ticket; ignored by rabbitMQ
        (ShortString $ exchangeName exchg) -- exchange
        (ShortString $ exchangeType exchg) -- typ
        (exchangePassive exchg) -- passive
        (exchangeDurable exchg) -- durable
        (exchangeAutoDelete exchg)  -- auto_delete
        (exchangeInternal exchg) -- internal
        False -- nowait
        (FieldTable (M.fromList [])))) -- arguments
    return ()

-- | deletes the exchange with the provided name
deleteExchange :: Channel -> String -> IO ()
deleteExchange chan myExchangeName = do
    (SimpleMethod Exchange_delete_ok) <- request chan (SimpleMethod (Exchange_delete
        1 -- ticket; ignored by rabbitMQ
        (ShortString myExchangeName) -- exchange
        False -- if_unused;  If set, the server will only delete the exchange if it has no queue bindings.
        False -- nowait
        ))
    return ()

----- QUEUE -----

-- | A record that contains the fields needed when creating a new queue using 'declareQueue'. The default values apply when you use 'newQueue'.
data QueueOpts = QueueOpts 
             {
                --must be set
                queueName :: String, -- ^ (default \"\"); the name of the queue; if left empty, the server will generate a new name and return it from the 'declareQueue' method
                
                --optional
                queuePassive :: Bool, -- ^ (default 'False'); If set, the server will not create the queue.  The client can use this to check whether a queue exists without modifying the server state.                
                queueDurable :: Bool, -- ^ (default 'True'); If set when creating a new queue, the queue will be marked as durable. Durable queues remain active when a server restarts. Non-durable queues (transient queues) are purged if/when a server restarts. Note that durable queues do not necessarily hold persistent messages, although it does not make sense to send persistent messages to a transient queue.
                queueExclusive :: Bool, -- ^ (default 'False'); Exclusive queues may only be consumed from by the current connection. Setting the 'exclusive' flag always implies 'auto-delete'.
                queueAutoDelete :: Bool -- ^ (default 'False'); If set, the queue is deleted when all consumers have finished using it. Last consumer can be cancelled either explicitly or because its channel is closed. If there was no consumer ever on the queue, it won't be deleted.
             }

-- | a 'QueueOpts' with defaults set; you should override at least 'queueName'.
newQueue :: QueueOpts
newQueue = QueueOpts "" False True False False

-- | creates a new queue on the AMQP server; can be used like this: @declareQueue channel newQueue {queueName = \"myQueue\"}@. 
--
-- Returns a tuple @(queueName, messageCount, consumerCount)@. 
-- @queueName@ is the name of the new queue (if you don't specify a queueName the server will autogenerate one). 
-- @messageCount@ is the number of messages in the queue, which will be zero for newly-created queues. @consumerCount@ is the number of active consumers for the queue.
declareQueue :: Channel -> QueueOpts -> IO (String, Int, Int)
declareQueue chan queue = do
    (SimpleMethod (Queue_declare_ok (ShortString qName) messageCount consumerCount)) <- request chan $ (SimpleMethod (Queue_declare 
            1 -- ticket
            (ShortString $ queueName queue)
            (queuePassive queue) 
            (queueDurable queue) 
            (queueExclusive queue) 
            (queueAutoDelete queue) 
            False -- no-wait; true means no answer from server
            (FieldTable (M.fromList []))))

    return (qName, fromIntegral messageCount, fromIntegral consumerCount)

-- | @bindQueue chan queueName exchangeName routingKey@ binds the queue to the exchange using the provided routing key
bindQueue :: Channel -> String -> String -> String -> IO ()
bindQueue chan myQueueName myExchangeName routingKey = do
    (SimpleMethod Queue_bind_ok) <- request chan (SimpleMethod (Queue_bind
        1 -- ticket; ignored by rabbitMQ
        (ShortString myQueueName)
        (ShortString myExchangeName)
        (ShortString routingKey)
        False -- nowait
        (FieldTable (M.fromList [])))) -- arguments

    return ()

-- | remove all messages from the queue; returns the number of messages that were in the queue
purgeQueue :: Channel -> String -> IO Word32
purgeQueue chan myQueueName = do
    (SimpleMethod (Queue_purge_ok msgCount)) <- request chan $ (SimpleMethod (Queue_purge
        1 -- ticket
        (ShortString myQueueName) -- queue
        False -- nowait
        ))
    return msgCount

-- | deletes the queue; returns the number of messages that were in the queue before deletion
deleteQueue :: Channel -> String -> IO Word32
deleteQueue chan myQueueName = do
    (SimpleMethod (Queue_delete_ok msgCount)) <- request chan $ (SimpleMethod (Queue_delete
        1 -- ticket
        (ShortString myQueueName) -- queue
        False -- if_unused
        False -- if_empty
        False -- nowait
        ))

    return msgCount


----- MSG (the BASIC class in AMQP) -----

type ConsumerTag = String

-- | specifies whether you have to acknowledge messages that you receive from 'consumeMsgs' or 'getMsg'. If you use 'Ack', you have to call 'ackMsg' or 'ackEnv' after you have processed a message, otherwise it might be delivered again in the future
data Ack = Ack | NoAck

ackToBool :: Ack -> Bool
ackToBool Ack = False
ackToBool NoAck = True

-- | @consumeMsgs chan queueName ack callback@ subscribes to the given queue and returns a consumerTag. For any incoming message, the callback will be run. If @ack == 'Ack'@ you will have to acknowledge all incoming messages (see 'ackMsg' and 'ackEnv')
--
-- NOTE: The callback will be run on the same thread as the channel thread (every channel spawns its own thread to listen for incoming data) so DO NOT perform any request on @chan@ inside the callback (however, you CAN perform requests on other open channels inside the callback, though I wouldn't recommend it).
-- Functions that can safely be called on @chan@ are 'ackMsg', 'ackEnv', 'rejectMsg', 'recoverMsgs'. If you want to perform anything more complex, it's a good idea to wrap it inside 'forkIO'.
consumeMsgs :: Channel -> String -> Ack -> ((Message,Envelope) -> IO ()) -> IO ConsumerTag
consumeMsgs chan myQueueName ack callback = do
    --generate a new consumer tag
    newConsumerTag <- (liftM show) $ modifyMVar (lastConsumerTag chan) $ \c -> return (c+1,c+1)

    --register the consumer
    modifyMVar_ (consumers chan) $ \c -> return $ M.insert newConsumerTag callback c

    writeAssembly chan (SimpleMethod $ Basic_consume
        1 -- ticket
        (ShortString myQueueName) -- queue
        (ShortString newConsumerTag) -- consumer_tag
        False -- no_local; If the no-local field is set the server will not send messages to the client that published them.
        (ackToBool ack) -- no_ack
        False -- exclusive; Request exclusive consumer access, meaning only this consumer can access the queue.
        True -- nowait
        )
    return newConsumerTag

-- | stops a consumer that was started with 'consumeMsgs'
cancelConsumer :: Channel -> ConsumerTag -> IO ()
cancelConsumer chan consumerTag = do
    (SimpleMethod (Basic_cancel_ok _)) <- request chan $ (SimpleMethod (Basic_cancel
        (ShortString consumerTag) -- consumer_tag
        False -- nowait
        ))

    --unregister the consumer
    modifyMVar_ (consumers chan) $ \c -> return $ M.delete consumerTag c


-- | @publishMsg chan exchangeName routingKey msg@ publishes @msg@ to the exchange with the provided @exchangeName@. The effect of @routingKey@ depends on the type of the exchange
--
-- NOTE: This method may temporarily block if the AMQP server requested us to stop sending content data (using the flow control mechanism). So don't rely on this method returning immediately
publishMsg :: Channel -> String -> String -> Message -> IO ()
publishMsg chan myExchangeName routingKey msg = do
    writeAssembly chan (ContentMethod (Basic_publish
            1 -- ticket; ignored by rabbitMQ
            (ShortString myExchangeName)
            (ShortString routingKey)
            False -- mandatory; if true, the server might return the msg, which is currently not handled
            False) --immediate; if true, the server might return the msg, which is currently not handled

            --TODO: add more of these to 'Message'
            (CHBasic
            (fmap ShortString $ msgContentType msg)
            Nothing
            Nothing
            (fmap deliveryModeToInt $ msgDeliveryMode msg) -- delivery_mode 
            Nothing
            (fmap ShortString $ msgCorrelationID msg)
            (fmap ShortString $ msgReplyTo msg)
            Nothing
            (fmap ShortString $ msgID msg)
            (msgTimestamp msg)
            Nothing        
            Nothing
            Nothing
            Nothing
            )
            
            (msgBody msg))
    return ()
    

-- | @getMsg chan ack queueName@ gets a message from the specified queue. If @ack=='Ack'@, you have to call 'ackMsg' or 'ackEnv' for any message that you get, otherwise it might be delivered again in the future (by calling 'recoverMsgs')
getMsg :: Channel -> Ack -> String -> IO (Maybe (Message, Envelope))
getMsg chan ack myQueueName = do
    ret <- request chan (SimpleMethod (Basic_get
        1 -- ticket
        (ShortString myQueueName) -- queue
        (ackToBool ack) -- no_ack
        ))

    case ret of
        ContentMethod (Basic_get_ok deliveryTag redelivered (ShortString myExchangeName) (ShortString routingKey) _) properties myMsgBody ->
            return $ Just $ (msgFromContentHeaderProperties properties myMsgBody,
                             Envelope {envDeliveryTag = deliveryTag, envRedelivered = redelivered,
                             envExchangeName = myExchangeName, envRoutingKey = routingKey, envChannel = chan})
        _ -> return Nothing


{- | @ackMsg chan deliveryTag multiple@ acknowledges one or more messages.

if @multiple==True@, the @deliverTag@ is treated as \"up to and including\", so that the client can acknowledge multiple messages with a single method call. If @multiple==False@, @deliveryTag@ refers to a single message. 

If @multiple==True@, and @deliveryTag==0@, tells the server to acknowledge all outstanding mesages.
-}
ackMsg :: Channel -> LongLongInt -> Bool -> IO ()
ackMsg chan deliveryTag multiple = 
    writeAssembly chan $ (SimpleMethod (Basic_ack
        deliveryTag -- delivery_tag
        multiple -- multiple
        ))
   
-- | Acknowledges a single message. This is a wrapper for 'ackMsg' in case you have the 'Envelope' at hand. 
ackEnv :: Envelope -> IO ()   
ackEnv env = ackMsg (envChannel env) (envDeliveryTag env) False

-- | @rejectMsg chan deliveryTag requeue@ allows a client to reject a message. It can be used to interrupt and cancel large incoming messages, or return untreatable  messages to their original queue. If @requeue==False@, the message will be discarded.  If it is 'True', the server will attempt to requeue the message.
-- 
-- NOTE: RabbitMQ 1.7 doesn't implement this command
rejectMsg :: Channel -> LongLongInt -> Bool -> IO ()
rejectMsg chan deliveryTag requeue = 
    writeAssembly chan $ (SimpleMethod (Basic_reject
        deliveryTag -- delivery_tag
        requeue -- requeue
        ))

-- | @recoverMsgs chan requeue@ asks the broker to redeliver all messages that were received but not acknowledged on the specified channel.
--If @requeue==False@, the message will be redelivered to the original recipient. If @requeue==True@, the server will attempt to requeue the message, potentially then delivering it to an alternative subscriber.
recoverMsgs :: Channel -> Bool -> IO ()
recoverMsgs chan requeue = 
    writeAssembly chan $ (SimpleMethod (Basic_recover
        requeue -- requeue
        ))

        
        
------------------- TRANSACTIONS (TX) --------------------------

-- | This method sets the channel to use standard transactions.  The client must use this method at least once on a channel before using the Commit or Rollback methods.
txSelect :: Channel -> IO ()
txSelect chan = do
    (SimpleMethod Tx_select_ok) <- request chan $ SimpleMethod Tx_select
    return ()
    
-- | This method commits all messages published and acknowledged in the current transaction.  A new transaction starts immediately after a commit.    
txCommit :: Channel -> IO ()
txCommit chan = do
    (SimpleMethod Tx_commit_ok) <- request chan $ SimpleMethod Tx_commit
    return ()

-- | This method abandons all messages published and acknowledged in the current transaction. A new transaction starts immediately after a rollback.    
txRollback :: Channel -> IO ()
txRollback chan = do
    (SimpleMethod Tx_rollback_ok) <- request chan $ SimpleMethod Tx_rollback
    return ()    
    
    
--------------------- FLOW CONTROL ------------------------


{- | @flow chan active@ tells the AMQP server to pause or restart the flow of content
    data. This is a simple flow-control mechanism that a peer can use
    to avoid overflowing its queues or otherwise finding itself receiving
    more messages than it can process.  
    
    If @active==True@ the server will start sending content data, if @active==False@ the server will stop sending content data.
    
    A new channel is always active by default.
    
    NOTE: RabbitMQ 1.7 doesn't implement this command.
    -}
flow :: Channel -> Bool -> IO ()
flow chan active = do
    (SimpleMethod (Channel_flow_ok _)) <- request chan $ SimpleMethod (Channel_flow active)
    return ()

   

   
-------------------------- MESSAGE / ENVELOPE ------------------
   
-- | contains meta-information of a delivered message (through 'getMsg' or 'consumeMsgs')
data Envelope = Envelope 
              {
                envDeliveryTag :: LongLongInt,
                envRedelivered :: Bool, 
                envExchangeName :: String,
                envRoutingKey :: String,
                envChannel :: Channel
              }

data DeliveryMode = Persistent -- ^ the message will survive server restarts (if the queue is durable)
                  | NonPersistent -- ^ the message may be lost after server restarts
    deriving Show

deliveryModeToInt :: (Num a) => DeliveryMode -> a
deliveryModeToInt NonPersistent = 1
deliveryModeToInt Persistent = 2

intToDeliveryMode :: (Num a) => a -> DeliveryMode
intToDeliveryMode 1 = NonPersistent
intToDeliveryMode 2 = Persistent

-- | An AMQP message
data Message = Message {
                msgBody :: BL.ByteString, -- ^ the content of your message
                msgDeliveryMode :: Maybe DeliveryMode, -- ^ see 'DeliveryMode'
                msgTimestamp :: Maybe Timestamp, -- ^ use in any way you like; this doesn't affect the way the message is handled
                msgID :: Maybe String, -- ^ use in any way you like; this doesn't affect the way the message is handled
                msgContentType :: Maybe String,
                msgReplyTo :: Maybe String,
                msgCorrelationID :: Maybe String
                }
    deriving Show

-- | a 'Msg' with defaults set; you should override at least 'msgBody'
newMsg :: Message    
newMsg = Message (BL.empty) Nothing Nothing Nothing Nothing Nothing Nothing

------------- ASSEMBLY -------------------------    
-- an assembly is a higher-level object consisting of several frames (like in amqp 0-10)
data Assembly = SimpleMethod MethodPayload
              | ContentMethod MethodPayload ContentHeaderProperties BL.ByteString --method, properties, content-data
    deriving Show
              
-- | reads all frames necessary to build an assembly              
readAssembly :: Chan FramePayload -> IO Assembly
readAssembly chan = do
    m <- readChan chan
    case m of 
        MethodPayload p -> --got a method frame
            if hasContent m 
                then do
                    --several frames containing the content will follow, so read them
                    (props, msg) <- collectContent chan
                    return $ ContentMethod p props msg     
                else do          
                    return $ SimpleMethod p
        x -> error $ "didn't expect frame: "++(show x)

-- | reads a contentheader and contentbodies and assembles them
collectContent :: Chan FramePayload -> IO (ContentHeaderProperties, BL.ByteString)
collectContent chan = do
    (ContentHeaderPayload _ _ bodySize props) <- readChan chan

    content <- collect $ fromIntegral bodySize
    return (props, BL.concat content)
  where
    collect x | x <= 0 = return []
    collect remData = do
        (ContentBodyPayload payload) <- readChan chan
        r <- collect (remData - (BL.length payload))
        return $ payload : r



------------ CONNECTION -------------------

{- general concept:
Each connection has its own thread. Each channel has its own thread.
Connection reads data from socket and forwards it to channel. Channel processes data and forwards it to application.
Outgoing data is written directly onto the socket.

Incoming Data: Socket -> Connection-Thread -> Channel-Thread -> Application
Outgoing Data: Application -> Socket
-}

data Connection = Connection {
                    connSocket :: Socket,
                    connChannels :: (MVar (IM.IntMap (Channel, ThreadId))), --open channels (channelID => (Channel, ChannelThread))
                    connMaxFrameSize :: Int, --negotiated maximum frame size
                    connClosed :: MVar (Maybe String),
                    connClosedLock :: MVar (), -- used by closeConnection to block until connection-close handshake is complete
                    connWriteLock :: MVar (), -- to ensure atomic writes to the socket
                    connClosedHandlers :: MVar [IO ()],
                    lastChannelID :: MVar Int --for auto-incrementing the channelIDs
                }


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
            case IM.lookup (fromIntegral chanID) cs of
                Just c -> writeChan (inQueue $ fst c) payload
                Nothing -> print $ "ERROR: channel not open "++(show chanID)

                
-- | @openConnection hostname virtualHost loginName loginPassword@ opens a connection to an AMQP server running on @hostname@.
-- @virtualHost@ is used as a namespace for AMQP resources (default is \"/\"), so different applications could use multiple virtual hosts on the same AMQP server
--
-- NOTE: If the login name, password or virtual host are invalid, this method will throw a 'ConnectionClosedException'. The exception will not contain a reason why the connection was closed, so you'll have to find out yourself.
openConnection :: String -> String -> String -> String -> IO Connection           
openConnection host vhost loginName loginPassword =
  openConnection' host 5672 vhost loginName loginPassword

-- | same as 'openConnection' but allows you to specify a non-default port-number as the 2nd parameter  
openConnection' :: String -> PortNumber -> String -> String -> String -> IO Connection
openConnection' host port vhost loginName loginPassword = do
    proto <- getProtocolNumber "tcp"
    sock <- socket AF_INET Stream proto
    addr <- inet_addr host
    connect sock (SockAddrInet port addr)
    NB.send sock $ toStrict $ BPut.runPut $ do
        BPut.putByteString $ BS.pack "AMQP"
        BPut.putWord8 1
        BPut.putWord8 1 --TCP/IP
        BPut.putWord8 9 --Major Version
        BPut.putWord8 1 --Minor Version


    -- S: connection.start
    Frame 0 (MethodPayload (Connection_start _ _ _ _ _)) <- readFrameSock sock 4096

    -- C: start_ok
    writeFrameSock sock start_ok
    -- S: tune
    Frame 0 (MethodPayload (Connection_tune _ frame_max _)) <- readFrameSock sock 4096
    -- C: tune_ok
    let maxFrameSize = (min 131072 frame_max)

    writeFrameSock sock (Frame 0 (MethodPayload 
        --TODO: handle channel_max
        (Connection_tune_ok 0 maxFrameSize 0)
        ))  
    -- C: open
    writeFrameSock sock open
    
    -- S: open_ok
    Frame 0 (MethodPayload (Connection_open_ok _)) <- readFrameSock sock $ fromIntegral maxFrameSize

    -- Connection established!

    --build Connection object
    myConnChannels <- newMVar IM.empty
    lastChanID <- newMVar 0
    cClosed <- newMVar Nothing
    writeLock <- newMVar ()
    ccl <- newEmptyMVar
    myConnClosedHandlers <- newMVar []
    let conn = Connection sock myConnChannels (fromIntegral maxFrameSize) cClosed ccl writeLock myConnClosedHandlers lastChanID

    --spawn the connectionReceiver
    forkIO $ CE.finally (connectionReceiver conn) 
            (do 
                -- try closing socket
                CE.catch (sClose sock) (\(_::CE.SomeException) -> return ())
                
                -- mark as closed
                modifyMVar_ cClosed $ \x -> return $ Just $ maybe "closed" id x
                
                --kill all channel-threads
                withMVar myConnChannels $ \cc -> mapM_ (\c -> killThread $ snd c) $ IM.elems cc
                withMVar myConnChannels $ \_ -> return $ IM.empty
                
                -- mark connection as closed, so all pending calls to 'closeConnection' can now return
                tryPutMVar ccl ()
                
                -- notify connection-close-handlers
                withMVar myConnClosedHandlers sequence
                
                )

    return conn

  where
    start_ok = (Frame 0 (MethodPayload (Connection_start_ok  (FieldTable (M.fromList []))
        (ShortString "AMQPLAIN") 
        --login has to be a table without first 4 bytes    
        (LongString (drop 4 $ BL.unpack $ runPut $ put $ FieldTable (M.fromList [(ShortString "LOGIN",FVLongString $ LongString loginName), (ShortString "PASSWORD", FVLongString $ LongString loginPassword)]))) 
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
    

-- | @addConnectionClosedHandler conn ifClosed handler@ adds a @handler@ that will be called after the connection is closed (either by calling @closeConnection@ or by an exception). If the @ifClosed@ parameter is True and the connection is already closed, the handler will be called immediately. If @ifClosed == False@ and the connection is already closed, the handler will never be called
addConnectionClosedHandler :: Connection -> Bool -> IO () -> IO ()
addConnectionClosedHandler conn ifClosed handler = do
    withMVar (connClosed conn) $ \cc -> do
        case cc of
            -- connection is already closed, so call the handler directly
            Just _ | ifClosed == True -> handler
            
            -- otherwise add it to the list
            _ -> modifyMVar_ (connClosedHandlers conn) $ \old -> return $ handler:old
        
            
    

    
readFrameSock :: Socket -> Int -> IO Frame
readFrameSock sock _ = do
    dat <- recvExact 7
    let len = fromIntegral $ peekFrameSize dat
    dat' <- recvExact (len+1) -- +1 for the terminating 0xCE
    let (frame, _, consumedBytes) = runGetState get (BL.append dat dat') 0


    if consumedBytes /= fromIntegral (len+8)
        then error $ "readFrameSock: parser should read "++show (len+8)++" bytes; but read "++show consumedBytes
        else return ()
    return frame

  where
    recvExact bytes = do
        b <- recvExact' bytes $ BL.empty
        if BL.length b /= fromIntegral bytes
            then error $ "recvExact wanted "++show bytes++" bytes; got "++show (BL.length b)++" bytes"
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
        
    

------------------------ CHANNEL -----------------------------

{- | A connection to an AMQP server is made up of separate channels. It is recommended to use a separate channel for each thread in your application that talks to the AMQP server (but you don't have to as channels are thread-safe)
-}
data Channel = Channel {
                    connection :: Connection, 
                    inQueue :: Chan FramePayload, --incoming frames (from Connection)
                    outstandingResponses :: Chan (MVar Assembly), -- for every request an MVar is stored here waiting for the response
                    channelID :: Word16, 
                    lastConsumerTag :: MVar Int, 
                    
                    chanActive :: Lock, -- used for flow-control. if lock is closed, no content methods will be sent
                    chanClosed :: MVar (Maybe String),
                    consumers :: MVar (M.Map String ((Message, Envelope) -> IO ())) -- who is consumer of a queue? (consumerTag => callback)
                }
                

msgFromContentHeaderProperties :: ContentHeaderProperties -> BL.ByteString -> Message
msgFromContentHeaderProperties
    (CHBasic content_type _ _ delivery_mode _ correlation_id reply_to _
             message_id timestamp _ _ _ _) myMsgBody =
    let msgId = fromShortString message_id
        contentType = fromShortString content_type
        replyTo = fromShortString reply_to
        correlationID = fromShortString correlation_id
        
        in
            Message myMsgBody (fmap intToDeliveryMode delivery_mode) timestamp msgId contentType replyTo correlationID
  where
    fromShortString (Just (ShortString s)) = Just s
    fromShortString _ = Nothing
    
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
                    let env =  Envelope {envDeliveryTag = deliveryTag, envRedelivered = redelivered,
                                    envExchangeName = myExchangeName, envRoutingKey = routingKey, envChannel = chan}

                    subscriber (msg, env)
                Nothing ->
                    -- got a message, but have no registered subscriber; so drop it
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
    handleAsync (ContentMethod (Basic_return _ _ _ _) _ _) =
        --TODO: implement handling
        -- this won't be called currently, because publishMsg sets "mandatory" and "immediate" to false
        print "BASIC.RETURN not implemented" 

-- closes the channel internally; but doesn't tell the server
closeChannel' :: Channel -> IO ()
closeChannel' c = do
    modifyMVar_ (connChannels $ connection c) $ \old -> return $ IM.delete (fromIntegral $ channelID c) old
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
   
    
   
-- | opens a new channel on the connection     
--
-- There's currently no closeChannel method, but you can always just close the connection (the maximum number of channels is 65535).
openChannel :: Connection -> IO Channel
openChannel c = do
    newInQueue <- newChan
    outRes <- newChan
    myLastConsumerTag <- newMVar 0
    ca <- newLock

    myChanClosed <- newMVar Nothing
    myConsumers <- newMVar M.empty 
    
    --get a new unused channelID
    newChannelID <- modifyMVar (lastChannelID c) $ \x -> return (x+1,x+1)
    
    let newChannel = Channel c newInQueue outRes (fromIntegral newChannelID) myLastConsumerTag ca myChanClosed myConsumers


    thrID <- forkIO $ CE.finally (channelReceiver newChannel)
        (closeChannel' newChannel)

    --add new channel to connection's channel map
    modifyMVar_ (connChannels c) (\oldMap -> return $ IM.insert newChannelID (newChannel, thrID) oldMap)
     
    (SimpleMethod Channel_open_ok) <- request newChannel (SimpleMethod (Channel_open (ShortString "")))
    return newChannel        

  


-- | writes multiple frames to the channel atomically
writeFrames :: Channel -> [FramePayload] -> IO ()
writeFrames chan payloads = 
    let conn = connection chan in
        withMVar (connChannels conn) $ \chans ->
            if IM.member (fromIntegral $ channelID chan) chans
                then
                    CE.catch 
                        -- ensure at most one thread is writing to the socket at any time
                        (withMVar (connWriteLock conn) $ \_ -> 
                            mapM_ (\payload -> writeFrameSock (connSocket conn) (Frame (channelID chan) payload)) payloads)
                        ( \(_ :: CE.IOException) -> do
                            CE.throwIO $ userError "connection not open"
                        )
                else do
                    CE.throwIO $ userError "channel not open"


   
    

writeAssembly' :: Channel -> Assembly -> IO ()        
writeAssembly' chan (ContentMethod m properties msg) = do
    -- wait iff the AMQP server instructed us to withhold sending content data (flow control)
    waitLock $ chanActive chan
    
    let !toWrite = 
           [(MethodPayload m),
            (ContentHeaderPayload 
                (getClassIDOf properties) --classID
                0 --weight is deprecated in AMQP 0-9
                (fromIntegral $ BL.length msg) --bodySize
                properties)] ++ 
            (if BL.length msg > 0 
             then do
                --split into frames of maxFrameSize
                map ContentBodyPayload
                    (splitLen msg (fromIntegral $ connMaxFrameSize $ connection chan)) 
             else []
            )
    writeFrames chan toWrite

  where
    splitLen str len | BL.length str > len = (BL.take len str):(splitLen (BL.drop len str) len)
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
        
        [CE.Handler (\ (_ :: AMQPException) -> throwMostRelevantAMQPException chan),
         CE.Handler (\ (_ :: CE.ErrorCall) -> throwMostRelevantAMQPException chan),
         CE.Handler (\ (_ :: CE.IOException) -> throwMostRelevantAMQPException chan)]
   
   


-- | sends an assembly and receives the response    
request :: Channel -> Assembly -> IO Assembly
request chan m = do
    res <- newEmptyMVar
    CE.catches (do
            withMVar (chanClosed chan) $ \cc -> do
                if isNothing cc
                    then do
                        writeChan (outstandingResponses chan) res 
                        writeAssembly' chan m
                    else CE.throwIO $ userError "closed"

            -- res might contain an exception, so evaluate it here
            !r <- takeMVar res
            return r
            )
        [CE.Handler (\ (_ :: AMQPException) -> throwMostRelevantAMQPException chan),
         CE.Handler (\ (_ :: CE.ErrorCall) -> throwMostRelevantAMQPException chan),
         CE.Handler (\ (_ :: CE.IOException) -> throwMostRelevantAMQPException chan)]

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
      



----------------------------- EXCEPTIONS ---------------------------

data AMQPException = 
    -- | the 'String' contains the reason why the channel was closed
    ChannelClosedException String
    | ConnectionClosedException String -- ^ String may contain a reason
  deriving (Typeable, Show, Ord, Eq)    

instance CE.Exception AMQPException    
