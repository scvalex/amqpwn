{-# LANGUAGE ScopedTypeVariables #-}

import Control.Concurrent ( forkIO, killThread )
import Control.Concurrent.MVar ( newEmptyMVar, putMVar, takeMVar, tryPutMVar )
import Control.Exception ( handle, bracket, IOException )
import Control.Monad ( replicateM, mapM_ )
import Network.AMQP ( Connection, openConnection, closeConnectionNormal
                    , openChannel, closeChannelNormal
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
                 addConnectionClosedHandler conn True (putMVar m (Right ()))
                 closeConnectionNormal conn
                 tid <- forkIO $ do
                         sleep 1
                         tryPutMVar m (Left "timeout")
                         return ()
                 v <- takeMVar m
                 case v of
                   Left err -> assertFailure err
                   Right ()  -> killThread tid
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
                   handle (\(ChannelClosedException "Normal") -> return ())
                          (closeChannelNormal ch)
             , "channelClosedConnection" ~: TestCase $ do
                 conn <- openDefaultConnection
                 ch <- openChannel conn
                 closeConnectionNormal conn
                 handle (\(ConnectionClosedException "Closed") -> return ())
                        (closeChannelNormal ch)
             ]

openDefaultConnection :: IO Connection
openDefaultConnection = openConnection "localhost" 5672 "/" "guest" "guest"

withConnection :: (Connection -> IO ()) -> IO ()
withConnection = bracket openDefaultConnection closeConnectionNormal
