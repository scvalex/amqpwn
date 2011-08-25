{-# LANGUAGE ScopedTypeVariables #-}

import Control.Concurrent ( forkIO, killThread )
import Control.Concurrent.MVar ( newEmptyMVar, putMVar, takeMVar, tryPutMVar )
import Control.Exception ( handle, bracket, IOException )
import Control.Monad ( replicateM, mapM_ )
import Data.String ( fromString )
import Network.AMQP ( Connection, openConnection, closeConnectionNormal
                    , addConnectionClosedHandler )
import Network.AMQP.Types ( AMQPException(..) )
import System.Exit ( exitFailure )
import System.Posix.Unistd ( sleep )
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
                 closeConnectionNormal =<< openDefaultConnection
             , "connectionOpenClose10" ~: TestCase $ do
                 conns <- replicateM 10 openDefaultConnection
                 mapM_ closeConnectionNormal conns
             , "connectionNoServer" ~: TestCase $ do
                 handle (\(_ :: IOException) -> return ()) $ do
                     openConnection "localhost" 5600 "/" "guest" "guest"
                     assertFailure "connected to non-existing broker"
             , "connectionWrongLogin" ~: TestCase $ do
                 handle (\(ConnectionClosedException _) -> return ()) $ do
                     openConnection "localhost" 5672 "/" "guest" "geust"
                     assertFailure "connected with wrong password"
             , "connectionCloseHandler" ~: TestCase $ do
                 conn <- openDefaultConnection
                 m <- newEmptyMVar
                 addConnectionClosedHandler conn (\e -> putMVar m (Right e))
                 closeConnectionNormal conn
                 tid <- forkIO $ do
                         sleep 1
                         tryPutMVar m (Left "timeout")
                         return ()
                 v <- takeMVar m
                 case v of
                   Left err ->
                       assertFailure err
                   Right (ConnectionClosedException "Normal") ->
                       killThread tid
             ]

openDefaultConnection :: IO Connection
openDefaultConnection = openConnection "localhost" 5672 "/" "guest" "guest"

withConnection :: (Connection -> IO ()) -> IO ()
withConnection = bracket openDefaultConnection closeConnectionNormal
