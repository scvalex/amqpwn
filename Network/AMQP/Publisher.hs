{-# LANGUAGE MultiParamTypeClasses #-}

module Network.AMQP.Publisher (
        -- * Opaque publisher type
        Publisher, runPublisher,

        -- * Publishing methods
        publish
    ) where

import Control.Concurrent ( ThreadId, forkIO )
import qualified Control.Exception as CE
import Control.Monad.IO.Class ( MonadIO(..) )
import Control.Monad.State.Lazy ( MonadState(..), StateT(..), evalStateT )
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.String ( IsString(..) )
import Network.AMQP.Connection ( openChannel, closeChannel, async )
import Network.AMQP.Types ( Connection, ChannelId, ChannelType(..)
                          , ExchangeName, RoutingKey
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
-- A unique message sequence number is returned (see Publisher
-- Confirms for details).
publish :: ExchangeName -> RoutingKey -> BL.ByteString -> Publisher Int
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

runPublisher :: Connection -> Publisher () -> IO ThreadId
runPublisher conn pub = do
  (chId, _) <- openChannel conn PublishingChannel
  let state = PState { getConnection = conn
                     , getChannelId  = chId
                     , getMsgSeqNo   = 1 }
  forkIO (evalStateT (unPublisher pub) state
          `CE.finally` closeChannel conn chId)
