module Network.AMQP.Types.Internal (
        -- * AMQP low-level types
        Octet, Bit, ShortInt, LongInt, LongLongInt,
        ShortString(..), LongString(..),

        -- * AMQP abstract types
        ChannelID, PayloadSize, Timestamp,

        -- * FieldTable
        FieldTable(..), FieldValue(..),

        -- * Decimals
        Decimals, DecimalValue(..)
    ) where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Char
import Data.Int
import qualified Data.Map as M
import Data.String ( IsString(..) )
import Text.Printf ( printf )


-- AMQP low-level types
type Octet = Word8
type Bit = Bool
type ShortInt = Word16
type LongInt = Word32
type LongLongInt = Word64

newtype ShortString = ShortString String
    deriving ( Ord, Eq )

instance Show ShortString where
    show (ShortString s) = s

instance IsString ShortString where
    fromString = ShortString . take 255

instance Binary ShortString where
    get = do
      len <- getWord8
      dat <- getByteString (fromIntegral len)
      return . ShortString $ BS.unpack dat
    put (ShortString x) = do
      let s = BS.pack $ take 255 x --ensure string isn't longer than 255 bytes
      putWord8 $ fromIntegral (BS.length s)
      putByteString s

newtype LongString = LongString String

instance Show LongString where
    show (LongString s) = s

instance IsString LongString where
    fromString = LongString

instance Binary LongString where
    get = do
      len <- getWord32be
      dat <- getByteString (fromIntegral len)
      return . LongString $ BS.unpack dat
    put (LongString x) = do
      putWord32be $ fromIntegral (length x)
      putByteString (BS.pack x)


-- AMQP abstract types
type ChannelID = ShortInt
type PayloadSize = LongInt
type Timestamp = LongLongInt


-- Field table
data FieldTable = FieldTable (M.Map ShortString FieldValue)
    deriving Show
instance Binary FieldTable where
    get = do
      len <- get :: Get LongInt --length of fieldValuePairs in bytes

      if len > 0
        then do
          fvp <- getLazyByteString (fromIntegral len)
          let !fields = readMany fvp

          return . FieldTable $ M.fromList fields
        else do
          return . FieldTable $ M.empty

    put (FieldTable fvp) = do
      let bytes = runPut (putMany $ M.toList fvp) :: BL.ByteString
      put ((fromIntegral $ BL.length bytes) :: LongInt)
      putLazyByteString bytes

-- Field value
data FieldValue = FVLongString LongString
                | FVSignedInt Int32
                | FVDecimalValue DecimalValue
                | FVTimestamp Timestamp
                | FVFieldTable FieldTable
                | FVBit Bool
                  deriving ( Show )

instance IsString FieldValue where
    fromString = FVLongString . fromString

-- FIXME: this can probably be generated from the spec.
-- In the meantime, here's the complete list:
--   * 't' ->  Boolean v
--   * 'b' ->  Signed 8-bit v
--   * 'B' ->  Unsigned 8-bit x
--   * 's' ->  Signed 16-bit (following RabbitMQ's example) v
--   * 'u' ->  Unsigned 16-bit x
--   * 'I' ->  Signed 32-bit v
--   * 'i' ->  Unsigned 32-bit x
--   * 'l' ->  Signed 64-bit (again, following RabbitMQ's example) v
--   * 'f' ->  32-bit float v
--   * 'd' ->  64-bit float v
--   * 'D' ->  unsigned Decimal v
--   * 'S' ->  Long string v
--   * 'A' ->  Nested Array v
--   * 'T' ->  unsigned Timestamp (u64) v
--   * 'F' ->  Nested Table v
--   * 'V' ->  Void v
--   * 'x' ->  Byte array v
-- See librabbitmq/amqp.h and src/rabbit_binary_generator.erl for details.
-- FIXME: also update the table in FramingTypes

instance Binary FieldValue where
    get = do
      fieldType <- getWord8
      case chr $ fromIntegral fieldType of
        'S' -> do
            s <- get :: Get LongString
            return $ FVLongString s
        'I' -> do
            i <- get :: Get Int32 -- FIXME: this should probably take
                                 -- endianess into accoutn
            return $ FVSignedInt i
        'D' -> do
            d <- get :: Get DecimalValue
            return $ FVDecimalValue d
        'T' -> do
            t <- get :: Get Timestamp
            return $ FVTimestamp t
        'F' -> do
            ft <- get :: Get FieldTable
            return $ FVFieldTable ft
        't' -> do
            b <- get :: Get Bool
            return $ FVBit b
        ef -> do
            fail $ printf "unknown field type: %s" (show ef)
    put (FVLongString s)   = put 'S' >> put s
    put (FVSignedInt i)    = put 'I' >> put i
    put (FVDecimalValue d) = put 'D' >> put d
    put (FVTimestamp t)    = put 'T' >> put t
    put (FVFieldTable t)   = put 'F' >> put t
    put (FVBit b)          = put 't' >> put b


-- Decimals
data DecimalValue = DecimalValue Decimals LongInt
    deriving Show
instance Binary DecimalValue where
    get = do
      a <- getWord8
      b <- get :: Get LongInt
      return $ DecimalValue a b
    put (DecimalValue a b) = put a >> put b

type Decimals = Octet


-- Helpers

-- | Perform runGet on a ByteString until the string is empty.
readMany :: (Show t, Binary t) => BL.ByteString -> [t]
readMany = runGet (readMany' [] 0)
    where
      readMany' :: (Binary t) => [t] -> Integer -> Get [t]
      readMany' _ 1000 = error "readMany overflow"
      readMany' acc overflow = do
        x <- get
        r <- remaining
        if r > 0
          then readMany' (x:acc) (overflow+1)
          else return (x:acc)

-- | Put all elements in the given list.
putMany :: (Binary b) => [b] -> PutM ()
putMany = mapM_ put
