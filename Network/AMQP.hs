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
    module Network.AMQP.Connection,
    module Network.AMQP.Channel,

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
import qualified Data.Map as M

import Control.Concurrent
import Control.Monad

import Network.AMQP.Channel
import Network.AMQP.Connection
import Network.AMQP.Framing
import Network.AMQP.Protocol
import Network.AMQP.Types



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

    writeAssembly chan (SimpleMethod $ undefined{-Basic_consume
        1 -- ticket
        (ShortString myQueueName) -- queue
        (ShortString newConsumerTag) -- consumer_tag
        False -- no_local; If the no-local field is set the server will not send messages to the client that published them.
        (ackToBool ack) -- no_ack
        False -- exclusive; Request exclusive consumer access, meaning only this consumer can access the queue.
        True -- nowait -}
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
