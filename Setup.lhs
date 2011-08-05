#!/usr/bin/env runhaskell

\begin{code}
import Distribution.Simple
import Codegen.Codegen ( run )

main :: IO ()
main = defaultMainWithHooks myHooks

myHooks :: UserHooks
myHooks = simpleUserHooks {
            buildHook = \pd lbi uh bf -> do
                         putStrLn "Generating FramingData..."
                         src <- run "Codegen/amqp0-9-1.xml"
                         writeFile "Network/AMQP/FramingData.hs" src
                         buildHook simpleUserHooks pd lbi uh bf
          }

\end{code}
