{-# LANGUAGE MultiParamTypeClasses, ScopedTypeVariables #-}

module Network.Messaging.Publisher (
        -- * The Publisher monad
        Publisher, runPublisher,

        -- * Publishing methods
        publish, waitForConfirms
    ) where

import Control.Concurrent ( myThreadId
                          , MVar, newMVar, tryTakeMVar, modifyMVar_
                          , readMVar )
import qualified Control.Exception as CE
import Control.Monad.IO.Class ( MonadIO(..) )
import Control.Monad.State.Lazy ( MonadState(..), StateT(..)
                                , evalStateT, gets )
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.Set as S
import Data.String ( IsString(..) )
import Network.Messaging.AMQP.Connection ( openChannel, closeChannel
                                         , request, async )
import Network.Messaging.AMQP.Types ( Connection, ChannelId, ChannelType(..)
                                    , ExchangeName, RoutingKey, MessageId
                                    , Method(..), MethodPayload(..)
                                    , ContentHeaderProperties(..) )
import Text.Printf ( printf )

data PState = PState { getConnection  :: Connection
                     , getChannelId   :: ChannelId
                     , getMsgSeqNo    :: Int
                     , getUnconfirmed :: MVar (S.Set MessageId)
                     , getWaiter      :: MVar () }

newtype Publisher a = Publisher { unPublisher :: StateT PState IO a }

instance Monad Publisher where
    return = Publisher . return
    x >>= f = Publisher $ unPublisher x >>= (unPublisher . f)

instance MonadIO Publisher where
    liftIO x = Publisher $ liftIO x

-- | Publish a message to the given exchange with the routing key set.
-- A unique (for this publisher) message id is returned (see Publisher
-- Confirms for details).
publish :: ExchangeName -> RoutingKey -> BL.ByteString -> Publisher MessageId
publish x rk content = Publisher $ do
  state@(PState { getConnection  = conn
                , getChannelId   = chId
                , getMsgSeqNo    = msn
                , getWaiter      = waiter
                , getUnconfirmed = unconfirmed }) <- get
  liftIO $ do
    tryTakeMVar waiter   -- block subsequent waits
    modifyMVar_ unconfirmed $ return . (S.insert msn)
    async conn chId $
          ContentMethod (BasicPublish 0 (fromString x)
                                      (fromString rk) True False)
                        (CHBasic Nothing Nothing Nothing (Just 2) Nothing
                                 Nothing Nothing Nothing Nothing Nothing
                                 Nothing Nothing Nothing Nothing)
                        content
  put $ state { getMsgSeqNo = msn + 1 }
  return msn

-- | Wait until all messages published so far have been either
-- acknowledged (via @basic.ack@) or rejected (via @basic.nack@ or
-- @basic.return@).
waitForConfirms :: Publisher (S.Set MessageId)
waitForConfirms = Publisher $ do
  waiter <- gets getWaiter
  liftIO $ readMVar waiter
  return S.empty

-- | Run the given publisher.
--
-- Important note: the thread that runs the publisher may receive
-- asynchronous exceptions from the connection, while 'runPublisher'
-- is being evaluated (if, for instance, it publishes to a
-- non-existing exchange).  To handle such exceptions, wrap the call
-- to 'runPublisher' in a 'catch'.  Once 'runPublisher' has finished,
-- the thread will not receive such exceptions from the connection.
--
-- Since 'runPublisher' will not return until the publisher returns,
-- it is probably best to run this on a dedicated thread.  So, for
-- example:
--
-- @
--     forkIO $ runPublisher connection publisher `CE.catch` exceptionHandler
-- @
runPublisher :: Connection -> Publisher a -> IO a
runPublisher conn pub = do
  tid <- myThreadId
  waiter <- newMVar ()
  unconfirmed <- newMVar S.empty
  (chId, _) <- openChannel conn (PublishingChannel tid ackHandler
                                                  nackHandler returnHandler)
  request conn . SimpleMethod $ ConfirmSelect False
  let state = PState { getConnection  = conn
                     , getChannelId   = chId
                     , getMsgSeqNo    = 1
                     , getWaiter      = waiter
                     , getUnconfirmed = unconfirmed }
  evalStateT (unPublisher pub) state
    `CE.finally` cleanup chId
      where
        cleanup chId = do
          closeChannel conn chId
        ackHandler (BasicAck tag _) = do
          printf "Received an ack for %d" tag
        nackHandler (BasicNack tag _ _) = do
          printf "Received a nack for %d" tag
        returnHandler (BasicReturn _ _ _ _) = do
          printf "Received a basic.return; fsck me"