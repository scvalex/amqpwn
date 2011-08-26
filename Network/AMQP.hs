module Network.AMQP (
        -- * Connections
        Connection,
        openConnection, closeConnection, closeConnectionNormal,
        addConnectionClosedHandler,

        -- * Queue operations
        declareQueue, declareQueueAnon, deleteQueue
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
  resp <- request conn . SimpleMethod $
         Queue_declare 0               -- ticket
                       (fromString qn) -- name
                       False           -- passive
                       True            -- durable
                       False           -- exclusive
                       False           -- auto-delete
                       False           -- no-wait
                       (FieldTable (M.fromList []))
  let !(SimpleMethod (Queue_declare_ok _ count _)) = resp
  return (fromIntegral count)

-- | Declare an anonymous queue.  Throw an exception on failure.
-- Return the name of the newly created queue.
declareQueueAnon :: Connection -> IO QueueName
declareQueueAnon conn = do
  resp <- request conn . SimpleMethod $
         Queue_declare 0               -- ticket
                       (fromString "") -- name
                       False           -- passive
                       True            -- durable
                       False           -- exclusive
                       False           -- auto-delete
                       False           -- no-wait
                       (FieldTable (M.fromList []))
  let !(SimpleMethod (Queue_declare_ok (ShortString name) _ _)) = resp
  return name

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
