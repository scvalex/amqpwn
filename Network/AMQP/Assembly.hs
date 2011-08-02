{-# LANGUAGE ScopedTypeVariables #-}

module Network.AMQP.Assembly (
        -- * Assemblies...
        readAssembly, writeAssembly, writeAssembly'
    ) where

import Control.Concurrent.STM ( atomically, TChan, readTChan )
import qualified Control.Exception as CE
import qualified Data.ByteString.Lazy.Char8 as LB
import Network.AMQP.Protocol ( methodHasContent, collectContent, writeFrames
                             , throwMostRelevantAMQPException )
import Network.AMQP.Helpers ( waitLock )
import Network.AMQP.Types ( Channel(..), FramePayload(..), Assembly(..)
                          , getClassIDOf, Connection(..), AMQPException(..) )
import Text.Printf ( printf )

-- | Read an entire assembly (multiple frames) and return it.
readAssembly :: TChan FramePayload -> IO Assembly
readAssembly chan = do
  m <- atomically $ readTChan chan
  case m of
    MethodPayload p ->           -- got a method frame
         if methodHasContent m
           then do
             (props, msg) <- collectContent chan
             return $ ContentMethod p props msg
           else do
             return $ SimpleMethod p
    unframe -> CE.throw . ConnectionClosedException $
                  printf "unexpected frame: %s" (show unframe)

writeAssembly' :: Channel -> Assembly -> IO ()
writeAssembly' chan (ContentMethod m properties msg) = do
  -- wait iff the AMQP server instructed us to withhold sending
  -- content data (flow control)
  atomically $ waitLock $ getChanActive chan
  let !toWrite = [ MethodPayload m
                 , ContentHeaderPayload (getClassIDOf properties) --classID
                                        0 --weight is deprecated in AMQP 0-9
                                        (fromIntegral $ LB.length msg) --bodySize
                                        properties
                 ] ++
                 (if LB.length msg > 0
                    then do
                      --split into frames of maxFrameSize
                      map ContentBodyPayload
                         (splitLen msg (fromIntegral $ getMaxFrameSize $ getConnection chan))
                    else [])
  writeFrames chan toWrite
      where
        splitLen str len | LB.length str > len = (LB.take len str):(splitLen (LB.drop len str) len)
        splitLen str _ = [str]

writeAssembly' chan (SimpleMethod m) = do
    writeFrames chan [MethodPayload m]

-- most exported functions in this module will use either 'writeAssembly' or 'request' to talk to the server
-- so we perform the exception handling here

-- | writes an assembly to the channel
writeAssembly :: Channel -> Assembly -> IO ()
writeAssembly chan m =
    CE.catches
          (writeAssembly' chan m)
          [ CE.Handler (\ (_ :: AMQPException) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.ErrorCall) -> throwMostRelevantAMQPException chan)
          , CE.Handler (\ (_ :: CE.IOException) -> throwMostRelevantAMQPException chan)
          ]
