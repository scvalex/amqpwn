#!/usr/bin/env runhaskell

\begin{code}
import Distribution.Simple

main :: IO ()
main = defaultMainWithHooks myHooks

myHooks :: UserHooks
myHooks = simpleUserHooks {
            buildHook = \pd lbi uh bf -> do
                         putStrLn "Custom build..."
                         buildHook simpleUserHooks pd lbi uh bf
          }

\end{code}
