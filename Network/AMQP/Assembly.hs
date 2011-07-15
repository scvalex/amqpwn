{-# LANGUAGE ScopedTypeVariables #-}

module Network.AMQP.Assembly (
        readAssembly, writeAssembly, writeAssembly'
    ) where

import Control.Concurrent.Chan ( Chan, readChan )
import qualified Control.Exception as CE
import qualified Data.ByteString.Lazy.Char8 as LB
import Network.AMQP.Protocol ( methodHasContent, collectContent, writeFrames
                             , throwMostRelevantAMQPException )
import Network.AMQP.Helpers ( waitLock )
import Network.AMQP.Types ( Channel(..), FramePayload(..), Assembly(..)
                          , getClassIDOf, Connection(..), AMQPException )

------------- ASSEMBLY -------------------------
-- | reads all frames necessary to build an assembly
readAssembly :: Chan FramePayload -> IO Assembly
readAssembly chan = do
  m <- readChan chan
  case m of
    MethodPayload p -> --got a method frame
      if methodHasContent m
        then do
          --several frames containing the content will follow, so read them
          (props, msg) <- collectContent chan
          return $ ContentMethod p props msg
        else do
          return $ SimpleMethod p
    x -> error $ "didn't expect frame: " ++ (show x)

writeAssembly' :: Channel -> Assembly -> IO ()
writeAssembly' chan (ContentMethod m properties msg) = do
  -- wait iff the AMQP server instructed us to withhold sending
  -- content data (flow control)
  waitLock $ chanActive chan
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
                         (splitLen msg (fromIntegral $ getMaxFrameSize $ connection chan))
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
