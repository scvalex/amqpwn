{-# LANGUAGE MultiParamTypeClasses, ScopedTypeVariables #-}

module Network.Messaging.Publisher (
        -- * The Publisher monad
        Publisher, runPublisher,

        -- * Publishing methods
        publish, waitForConfirms
    ) where

import Control.Concurrent ( myThreadId )
import qualified Control.Exception as CE
import Control.Monad.IO.Class ( MonadIO(..) )
import Control.Monad.State.Lazy ( MonadState(..), StateT(..), evalStateT )
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.Set as S
import Data.String ( IsString(..) )
import Network.Messaging.AMQP.Connection ( openChannel, closeChannel
                                         , request, async )
import Network.Messaging.AMQP.Types ( Connection, ChannelId, ChannelType(..)
                                    , ExchangeName, RoutingKey, MessageId
                                    , Method(..), MethodPayload(..)
                                    , ContentHeaderProperties(..) )

data PState = PState { getConnection :: Connection
                     , getChannelId  :: ChannelId
                     , getMsgSeqNo   :: Int }

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
  state@(PState { getConnection = conn,
                  getChannelId = chId,
                  getMsgSeqNo = msn }) <- get
  liftIO $ async conn chId $
            ContentMethod (Basic_publish 0 (fromString x)
                                         (fromString rk) True False)
                          (CHBasic Nothing Nothing Nothing (Just 2) Nothing
                                   Nothing Nothing Nothing Nothing Nothing
                                   Nothing Nothing Nothing Nothing)
                          content
  put $ state { getMsgSeqNo = msn + 1 }
  return msn

waitForConfirms :: Publisher (S.Set MessageId)
waitForConfirms = undefined

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
  (chId, _) <- openChannel conn (PublishingChannel tid)
  request conn . SimpleMethod $ Confirm_select False
  let state = PState { getConnection = conn
                     , getChannelId  = chId
                     , getMsgSeqNo   = 1 }
  evalStateT (unPublisher pub) state
    `CE.finally` cleanup chId
      where
        cleanup chId = do
          closeChannel conn chId
