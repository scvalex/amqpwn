{-# LANGUAGE ScopedTypeVariables #-}

import Control.Exception ( handle, IOException )
import Network.AMQP ( openConnection, closeConnection )
import Network.AMQP.Types ( AMQPException(..) )
import System.Exit ( exitFailure )
import Test.HUnit

main :: IO ()
main = do
  counts <- runTestTT tests
  if (failures counts + errors counts == 0)
     then do
       putStrLn "All tests pass :)"
     else do
       putStrLn "Failures or errors occured :'("
       exitFailure

tests = test [ "alwaysPass" ~: TestCase $ do
                 return ()
             , "connectionOpenClose" ~: TestCase $ do
                 conn <- openConnection "127.0.0.1" (fromIntegral 5672) "/"
                                        "guest" "guest"
                 closeConnection conn
             , "connectionNoServer" ~: TestCase $ do
                 handle (\(_ :: IOException) -> return ()) $ do
                     openConnection "127.0.0.1" (fromIntegral 5600) "/"
                                    "guest" "guest"
                     assertFailure "connected to non-existing broker"
             , "connectionWrongLogin" ~: TestCase $ do
                 handle (\(ConnectionClosedException _) -> return ()) $ do
                     openConnection "127.0.0.1" (fromIntegral 5672) "/"
                                    "guest" "geust"
                     assertFailure "connected with wrong password"
             ]
