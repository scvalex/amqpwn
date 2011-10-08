module Network.Messaging (
        -- * Connections
        Connection,
        openConnection, closeConnection, closeConnectionNormal,
        addConnectionClosedHandler,

        -- * Queue operations
        QueueName,
        declareQueue, declareAnonQueue, deleteQueue,

        -- * Exchange operations
        ExchangeName, ExchangeType,
        declareExchange, deleteExchange,

        -- * Bindings
        RoutingKey,
        bindQueue, unbindQueue,
        bindExchange, unbindExchange,

        -- * Publishing
        Publisher, runPublisher, publish,

        -- * Exceptions
        AMQPException(..)
    ) where

import qualified Data.Map as M
import Data.String ( fromString )
import Network.Messaging.AMQP.Connection
import Network.Messaging.Publisher
import Network.Messaging.AMQP.Types

-- | Declare a queue with the specified name.  Throw an exception on
-- failure.  Applications of this function are idempotent
-- (i.e. calling it multiple times is equivalent to calling it once).
-- Return the number of messages already on the queue, if it exists,
-- or 0, otherwise.
declareQueue :: Connection -> QueueName -> IO Int
declareQueue conn qn = do
  (_, count) <- declareQueueInternal conn qn
  return count

-- | Declare an anonymous queue.  Throw an exception on failure.
-- Return the name of the newly created queue.
declareAnonQueue :: Connection -> IO QueueName
declareAnonQueue conn = do
  (name, _) <- declareQueueInternal conn ""
  return name

-- | Declare a queue with the given name.  Return its name (useful for
-- anonymous queues) and the number of messages on it.
declareQueueInternal :: Connection -> QueueName -> IO (QueueName, Int)
declareQueueInternal conn qn = do
  resp <- request conn . SimpleMethod $
         QueueDeclare 0               -- ticket
                      (fromString qn) -- name
                      False           -- passive
                      True            -- durable
                      False           -- exclusive
                      False           -- auto-delete
                      False           -- nowait
                      (FieldTable (M.fromList []))
  let !(SimpleMethod (QueueDeclareOk (ShortString name) count _)) = resp
  return (name, (fromIntegral count))

-- | Delete the queue with the specified name.  Throw an exception if
-- the queue does not exist.  Return the number of messages on the
-- queue when it was deleted.
deleteQueue :: Connection -> QueueName -> IO Int
deleteQueue conn qn = do
  resp <- request conn . SimpleMethod $
         QueueDelete 0               -- ticket
                     (fromString qn) -- name
                     False           -- if-unused
                     False           -- if-empty
                     False           -- nowait
  let !(SimpleMethod (QueueDeleteOk count)) = resp
  return (fromIntegral count)

-- | Declare an exchange with the specified name, type and internal
-- setting.  Throw an exception on failure.
declareExchange :: Connection
                -> ExchangeName
                -> ExchangeType
                -> Bool          -- ^ internal
                -> IO ()
declareExchange conn en et internal = do
  resp <- request conn . SimpleMethod $
         ExchangeDeclare 0               -- ticket
                         (fromString en) -- name
                         (fromString et) -- type
                         False           -- passive
                         True            -- durable
                         False           -- auto-delete
                         internal        -- guess what this is
                         False           -- nowait
                         (FieldTable (M.fromList []))
  let !(SimpleMethod ExchangeDeclareOk) = resp
  return ()

-- | Delete the exchange with the given name.  Throw an exception if
-- the exchange does not exist.
deleteExchange :: Connection -> ExchangeName -> IO ()
deleteExchange conn en = do
  resp <- request conn . SimpleMethod $
         ExchangeDelete 0               -- ticket
                        (fromString en) -- name
                        False           -- if-unused
                        False           -- nowait
  let !(SimpleMethod ExchangeDeleteOk) = resp
  return ()

-- | Bind a queue to an exchange with the given routing key.  Throw an
-- exception on failure.
bindQueue :: Connection -> QueueName -> ExchangeName -> RoutingKey -> IO ()
bindQueue conn qn en rk = do
  resp <- request conn . SimpleMethod $
         QueueBind 0               -- ticket
                   (fromString qn) -- queue name
                   (fromString en) -- exchange name
                   (fromString rk) -- routing key
                   False           -- nowait
                   (FieldTable (M.fromList []))
  let !(SimpleMethod QueueBindOk) = resp
  return ()

-- | Unbind a queue from an exchange with the given routing key.
-- Throw an exception if the binding doesn't exist.
unbindQueue :: Connection -> QueueName -> ExchangeName -> RoutingKey -> IO ()
unbindQueue conn qn en rk = do
  resp <- request conn . SimpleMethod $
         QueueUnbind 0               -- ticket
                     (fromString qn) -- queue name
                     (fromString en) -- exchange name
                     (fromString rk) -- routing key
                     (FieldTable (M.fromList []))
  let !(SimpleMethod QueueUnbindOk) = resp
  return ()

-- | Bind an exchange to another exchange with the given routing key.
-- Throw an exception on failure.
bindExchange :: Connection -> ExchangeName -> ExchangeName -> RoutingKey -> IO ()
bindExchange conn en1 en2 rk = do
  resp <- request conn . SimpleMethod $
         ExchangeBind 0                -- ticket
                      (fromString en1) -- name of first exchange
                      (fromString en2) -- name of second exchange
                      (fromString rk)  -- routing key
                      False            -- nowait
                      (FieldTable (M.fromList []))
  let !(SimpleMethod ExchangeBindOk) = resp
  return ()

-- | Unbind an exchange from another exchange with the given routing
-- key.  Throw an exception if the binding doesn't exist.
unbindExchange :: Connection -> ExchangeName -> ExchangeName -> RoutingKey -> IO ()
unbindExchange conn en1 en2 rk = do
  resp <- request conn . SimpleMethod $
         ExchangeUnbind 0                -- ticket
                        (fromString en1) -- name of first exchange
                        (fromString en2) -- name of second exchange
                        (fromString rk)  -- routing key
                        False            -- nowait
                        (FieldTable (M.fromList []))
  let !(SimpleMethod ExchangeUnbindOk) = resp
  return ()
