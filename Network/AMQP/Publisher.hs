{-# LANGUAGE MultiParamTypeClasses #-}

module Network.AMQP.Publisher (
        -- * Opaque publisher type
        Publisher, runPublisher,

        -- * Publishing methods
        publish, waitForConfirms
    ) where

import Control.Concurrent ( ThreadId, forkIO )
import Control.Concurrent.STM ( atomically
                              , newEmptyTMVar, takeTMVar, putTMVar )
import qualified Control.Exception as CE
import Control.Monad.IO.Class ( MonadIO(..) )
import Control.Monad.State.Lazy ( MonadState(..), StateT(..), evalStateT )
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.Set as S
import Data.String ( IsString(..) )
import Network.AMQP.Connection ( openChannel, closeChannel, async )
import Network.AMQP.Types ( Connection, ChannelId, ChannelType(..)
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

-- | Runs the given publisher on a dedicated thread.
runPublisher :: Connection -> Publisher () -> IO ThreadId
runPublisher conn pub = do
  chIdTV <- atomically $ newEmptyTMVar
  tid <- forkIO $ publisherPrelaunch chIdTV
  (chId, _) <- openChannel conn (PublishingChannel tid)
  atomically $ putTMVar chIdTV chId
  return tid
    where
      publisherPrelaunch chIdTV = do
        chId <- atomically $ takeTMVar chIdTV
        let state = PState { getConnection = conn
                           , getChannelId  = chId
                           , getMsgSeqNo   = 1 }
        forkIO (evalStateT (unPublisher pub) state
                `CE.finally` closeChannel conn chId)
        return ()
