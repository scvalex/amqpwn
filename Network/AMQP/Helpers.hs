module Network.AMQP.Helpers (
        -- * ByteString manipulation
        toStrict, toLazy,
    ) where

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL

-- | Convert a lazy ByteString to a strict one.
toStrict :: BL.ByteString -> BS.ByteString
toStrict x = BS.concat $ BL.toChunks x

-- | Convert a strict ByteString to a lazy one.
toLazy :: BS.ByteString -> BL.ByteString
toLazy x = BL.fromChunks [x]
