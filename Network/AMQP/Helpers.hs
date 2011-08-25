module Network.AMQP.Helpers (
        -- * ByteString manipulation
        toStrict, toLazy,

        -- * Things that should have been in STM
        modifyTVar, withTMVarIO
    ) where

import Control.Concurrent.STM ( STM, atomically
                              , TVar, writeTVar, readTVar
                              , TMVar, takeTMVar, putTMVar )
import qualified Control.Exception as CE
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL

-- | Convert a lazy ByteString to a strict one.
toStrict :: BL.ByteString -> BS.ByteString
toStrict x = BS.concat $ BL.toChunks x

-- | Convert a strict ByteString to a lazy one.
toLazy :: BS.ByteString -> BL.ByteString
toLazy x = BL.fromChunks [x]

-- | Modify a TVar in-place.
modifyTVar :: TVar a -> (a -> a) -> STM ()
modifyTVar var f = writeTVar var . f =<< readTVar var

-- | Take a TMVar, run an action with it and put it back.  The action
-- has exclusive access to the value while it is executing.
withTMVarIO :: TMVar a -> (a -> IO b) -> IO b
withTMVarIO var act = do
  val <- atomically $ takeTMVar var
  act val `CE.finally` atomically (putTMVar var val)
