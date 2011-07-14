import Network.AMQP ( openConnection, closeConnection )
import System.Exit ( exitFailure )
import Test.HUnit

main :: IO ()
main = do
  counts <- runTestTT tests
  if (failures counts + errors counts == 0)
     then do
       putStrLn "All test pass :)"
     else do
       putStrLn "Failures or errors occured :'("
       exitFailure

tests = test [ "alwaysPass" ~: TestCase $ do
                 return ()
             , "connectionOpenClose" ~: TestCase $ do
                 conn <- openConnection "127.0.0.1" (fromIntegral 5672) "/"
                                        "guest" "guest"
                 closeConnection conn
             ]
