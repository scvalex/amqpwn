{-# LANGUAGE MultiParamTypeClasses #-}

module Network.AMQP.Publisher (
        -- * Opaque publisher type
        Publisher, newPublisher,

        -- * Publishing methods
        publish
    ) where

import Control.Concurrent ( ThreadId, forkIO )
import Control.Monad.IO.Class ( MonadIO(..) )
import Control.Monad.State.Lazy ( MonadState(..), StateT, evalStateT )
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.String ( IsString(..) )
import Network.AMQP.Connection ( openChannel, async )
import Network.AMQP.Types ( Connection, ChannelId, ChannelType(..)
                          , ExchangeName, RoutingKey
                          , Method(..), MethodPayload(..)
                          , ContentHeaderProperties(..) )

data PState = PState { getConnection :: Connection
                     , getChannelId  :: ChannelId
                     , getMsgSeqNo   :: Int }

type Publisher a = StateT PState IO a

-- | Publish a message to the given exchange with the routing key set.
-- A unique message sequence number is returned (see Publisher
-- Confirms for details).
publish :: ExchangeName -> RoutingKey -> BL.ByteString -> Publisher Int
publish x rk content = do
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

newPublisher :: Connection -> Publisher () -> IO ThreadId
newPublisher conn pub = do
  (chId, _) <- openChannel conn PublishingChannel
  let state = PState { getConnection = conn
                     , getChannelId  = chId
                     , getMsgSeqNo   = 1 }
  forkIO $ evalStateT pub state
