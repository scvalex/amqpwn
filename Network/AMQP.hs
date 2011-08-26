module Network.AMQP (
        -- * Connections
        Connection,
        openConnection, closeConnection, closeConnectionNormal,
        addConnectionClosedHandler,

        -- * Queue operations
        QueueName,
        declareQueue, declareQueueAnon, deleteQueue,

        -- * Exchange operations
        ExchangeName, ExchangeType,
        declareExchange, deleteExchange
    ) where

import qualified Data.Map as M
import Data.String ( fromString )
import Network.AMQP.Connection
import Network.AMQP.Types

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
declareQueueAnon :: Connection -> IO QueueName
declareQueueAnon conn = do
  (name, _) <- declareQueueInternal conn ""
  return name

-- | Declare a queue with the given name.  Return its name (useful for
-- anonymous queues) and the number of messages on it.
declareQueueInternal :: Connection -> QueueName -> IO (QueueName, Int)
declareQueueInternal conn qn = do
  resp <- request conn . SimpleMethod $
         Queue_declare 0               -- ticket
                       (fromString qn) -- name
                       False           -- passive
                       True            -- durable
                       False           -- exclusive
                       False           -- auto-delete
                       False           -- nowait
                       (FieldTable (M.fromList []))
  let !(SimpleMethod (Queue_declare_ok (ShortString name) count _)) = resp
  return (name, (fromIntegral count))

-- | Delete the queue with the specified name.  Throw an exception if
-- the queue does not exist.  Return the number of messages on the
-- queue when it was deleted.
deleteQueue :: Connection -> QueueName -> IO Int
deleteQueue conn qn = do
  resp <- request conn . SimpleMethod $
         Queue_delete 0               -- ticket
                      (fromString qn) -- name
                      False           -- if-unused
                      False           -- if-empty
                      False           -- nowait
  let !(SimpleMethod (Queue_delete_ok count)) = resp
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
         Exchange_declare 0               -- ticket
                          (fromString en) -- name
                          (fromString et) -- type
                          False           -- passive
                          True            -- durable
                          False           -- auto-delete
                          internal        -- guess what this is
                          False           -- nowait
                          (FieldTable (M.fromList []))
  let !(SimpleMethod Exchange_declare_ok) = resp
  return ()

-- | Delete the exchange with the given name.  Throw an exception if
-- the exchange does not exist.
deleteExchange :: Connection -> ExchangeName -> IO ()
deleteExchange conn en = do
  resp <- request conn . SimpleMethod $
         Exchange_delete 0               -- ticket
                         (fromString en) -- name
                         False           -- if-unused
                         False           -- nowait
  let !(SimpleMethod Exchange_delete_ok) = resp
  return ()
