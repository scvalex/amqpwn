module Network.AMQP.Helpers (
        -- * ByteString manipulation
        toStrict, toLazy,

        -- * Things that should have been in STM
        modifyTVar, withTMVarIO
    ) where

import qualified Control.Concurrent.STM as STM
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL

-- | Convert a lazy ByteString to a strict one.
toStrict :: BL.ByteString -> BS.ByteString
toStrict x = BS.concat $ BL.toChunks x

-- | Convert a strict ByteString to a lazy one.
toLazy :: BS.ByteString -> BL.ByteString
toLazy x = BL.fromChunks [x]

-- | Modify a TVar in-place.
modifyTVar :: STM.TVar a -> (a -> a) -> STM.STM ()
modifyTVar var f = STM.atomically $ STM.writeTVar var . f =<< STM.readTVar var

-- | Take a TMVar, run an action with it and put it back.  The action
-- has exclusive access to the value while it is executing.
withTMVarIO :: STM.TMVar a -> (a -> IO b) -> IO b
withTMVarIO var act = do
  val <- STM.atomically $ takeTMVar var
  res <- act val
  STM.atomically $ putTMVar var val
  return res
