module Network.AMQP.Helpers (
        -- * ByteString manipulation
        toStrict, toLazy,

        -- * Locks, etc.
        Lock, newLock, openLock, closeLock, waitLock, killLock
    ) where

import Control.Applicative ( Applicative(..), (<$>) )
import Control.Concurrent.STM ( STM, TMVar, newTMVar, tryPutTMVar, putTMVar
                              , tryTakeTMVar, readTMVar )
import Control.Monad ( unless )
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL

-- | Convert a lazy ByteString to a strict one.
toStrict :: BL.ByteString -> BS.ByteString
toStrict x = BS.concat $ BL.toChunks x

-- | Convert a strict ByteString to a lazy one.
toLazy :: BS.ByteString -> BL.ByteString
toLazy x = BL.fromChunks [x]

-- | If the lock is open, calls to waitLock will immediately return.
-- If it is closed, calls to waitLock will block.  If the lock is
-- killed, it will always be open and can't be closed anymore.
data Lock = Lock (TMVar Bool) (TMVar ())

-- | Create an (alive, open) lock.
newLock :: STM Lock
newLock = Lock <$> (newTMVar False) <*> (newTMVar ())

-- | Open the given lock.  You may open a lock as many times as you
-- please.
openLock :: Lock -> STM ()
openLock (Lock _ b) = tryPutTMVar b () >> return ()

-- | Close the given lock.  You may always close a lock, but closing a
-- killed lock is a no-op.
closeLock :: Lock -> STM ()
closeLock (Lock a b) = do
  killed <- readTMVar a
  unless killed $ tryTakeTMVar b >> return ()
  return ()

-- | Wait until the given lock is open.
waitLock :: Lock -> STM ()
waitLock (Lock _ b) = readTMVar b

-- | Kill the given lock and open it.
killLock :: Lock -> STM Bool
killLock (Lock a b) = do
  putTMVar a True
  tryPutTMVar b ()
