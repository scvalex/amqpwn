{-# LANGUAGE ScopedTypeVariables #-}

import Control.Concurrent ( forkIO, killThread )
import Control.Concurrent.MVar ( newEmptyMVar, putMVar, takeMVar, tryPutMVar )
import Control.Exception ( handle, bracket, IOException )
import Control.Monad ( replicateM, mapM_ )
import Data.String ( fromString )
import Network.AMQP ( Connection, openConnection, closeConnectionNormal
                    , Channel, openChannel, closeChannelNormal
                    , addConnectionClosedHandler, request, Method(..)
                    , MethodPayload(..) )
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
             , "channelLifecycle" ~: TestCase $ do
                 withConnection $ \conn -> do
                   ch <- openChannel conn
                   closeChannelNormal ch
             , "channelLifecycle10" ~: TestCase $ do
                 withConnection $ \conn -> do
                   chs <- replicateM 10 (openChannel conn)
                   mapM_ closeChannelNormal chs
             , "channelDoublyClosed" ~: TestCase $ do
                 withConnection $ \conn -> do
                   ch <- openChannel conn
                   closeChannelNormal ch
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     closeChannelNormal ch
                     assertFailure "managed to close closed channel"
             , "channelClosedConnection" ~: TestCase $ do
                 conn <- openDefaultConnection
                 ch <- openChannel conn
                 closeConnectionNormal conn
                 handle (\(ConnectionClosedException _) -> return ()) $ do
                   closeChannelNormal ch
                   assertFailure "closed channel on closed connection"
             , "prohibitedMethod" ~: TestCase $ do
                 withChannel $ \ch -> do
                   handle (\(ClientException _) -> return ()) $ do
                     request ch (SimpleMethod (Channel_open (fromString "")))
                     assertFailure "sending channel.open worked"
                   return ()
             ]

openDefaultConnection :: IO Connection
openDefaultConnection = openConnection "localhost" 5672 "/" "guest" "guest"

withConnection :: (Connection -> IO ()) -> IO ()
withConnection = bracket openDefaultConnection closeConnectionNormal

withChannel :: (Channel -> IO ()) -> IO ()
withChannel act = withConnection $ \conn -> do
                    bracket (openChannel conn) closeChannelNormal act
