#!/usr/bin/env runhaskell

\begin{code}
{-# LANGUAGE ScopedTypeVariables #-}

import Codegen.Codegen ( run )
import qualified Control.Exception as CE
import Distribution.Simple
import System.Directory ( getModificationTime )

main :: IO ()
main = defaultMainWithHooks myHooks

myHooks :: UserHooks
myHooks = simpleUserHooks {
            buildHook = \pd lbi uh bf -> do
                          maybeGenerateFramingData
                          buildHook simpleUserHooks pd lbi uh bf
          }

framingDataFile, amqpSpecFile, codegenFile :: FilePath
framingDataFile = "Network/AMQP/FramingData.hs"
amqpSpecFile = "Codegen/amqp0-9-1.xml"
codegenFile = "Codegen/Codegen.hs"

maybeGenerateFramingData :: IO ()
maybeGenerateFramingData = do
  CE.handle (\(e :: IOError) -> generateFramingData) $ do
      mod1 <- getModificationTime framingDataFile
      mod2 <- getModificationTime amqpSpecFile
      mod3 <- getModificationTime codegenFile
      if any (mod1 <) [mod2, mod3]
        then fail "FramingData out of date"
        else return ()

generateFramingData :: IO ()
generateFramingData = do
  putStrLn "Generating FramingData..."
  src <- run amqpSpecFile
  writeFile framingDataFile src

\end{code}
