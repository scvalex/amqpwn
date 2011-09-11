{-# LANGUAGE ScopedTypeVariables #-}

import Control.Concurrent ( forkIO, killThread )
import Control.Concurrent.MVar ( newEmptyMVar, putMVar, takeMVar, tryPutMVar )
import Control.Exception ( IOException, SomeException
                         , bracket, finally, handle, throw )
import Control.Monad ( forM, mapM_, replicateM )
import Data.String ( fromString )
import Network.AMQP ( Connection, openConnection, closeConnectionNormal
                    , addConnectionClosedHandler
                    , declareQueue, declareAnonQueue, deleteQueue
                    , declareExchange, deleteExchange
                    , bindQueue, unbindQueue
                    , bindExchange, unbindExchange )
import Network.AMQP.Types ( AMQPException(..) )
import System.Exit ( exitFailure )
import System.Posix.Unistd ( sleep )
import Test.HUnit

main :: IO ()
main = do
  counts <- runTestTT $ test [tests, stressTests]
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
             , "queueDeclare" ~: TestCase $ do
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   return ()
             , "queueDoubleDeclare" ~: TestCase $ do
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   declareQueue conn "test-queue"
                   return ()
             , "queueDeclareDelete" ~: TestCase $ do
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   deleteQueue conn "test-queue"
                   return ()
             , "queueDelete" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteQueue conn "test-queue"
                     assertFailure "deleted non-existing queue"
             , "queueDeleteDeclare" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteQueue conn "test-queue"
                     return ()
                   declareQueue conn "test-queue"
                   deleteQueue conn "test-queue"
                   return ()
             , "exchangeDeclare" ~: TestCase $ do
                 withConnection $ \conn -> do
                   declareExchange conn "test-exchange" "direct" False
                   return ()
             , "exchangeDeclareDelete" ~: TestCase $ do
                 withConnection $ \conn -> do
                   declareExchange conn "test-exchange" "direct" False
                   deleteExchange conn "test-exchange"
                   return ()
             , "exchangeDelete" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteExchange conn "test-exchange"
                     assertFailure "deleted non-existing exchange"
             , "exchangeDeleteDeclare" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteExchange conn "test-exchange"
                     return ()
                   declareExchange conn "test-exchange" "direct" False
                   deleteExchange conn "test-exchange"
                   return ()
             , "queueBindUnbind" ~: TestCase $ do
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   (do
                     bindQueue conn "test-queue" "amq.direct" ""
                     unbindQueue conn "test-queue" "amq.direct" "")
                    `finally`
                      deleteQueue conn "test-queue"
             , "queueUnbindNonExisting" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     unbindQueue conn "test-queue" "amq.direct" ""
                     assertFailure "unbound non-existing queue"
             , "queueUnbindNonExisting2" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     declareQueue conn "test-queue"
                     (do
                       unbindQueue conn "test-queue" "amq.direct" ""
                       assertFailure "unbound non-existing binding")
                      `finally`
                        deleteQueue conn "test-queue"
             , "exchangeBindUnbind" ~: TestCase $ do
                 withConnection $ \conn -> do
                   bindExchange conn "amq.fanout" "amq.direct" ""
                   unbindExchange conn "amq.fanout" "amq.direct" ""
             , "exchangeUnbindNonExisting" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     unbindExchange conn "no-such-exchange" "amq.direct" ""
                     assertFailure "unbound non-existing exchange"
             , "exchangeUnbindNonExisting2" ~: TestCase $ do
                 withConnection $ \conn -> do
                   handle (\(ChannelClosedException _) -> return ()) $ do
                     unbindExchange conn "amq.fanout" "amq.direct" "pfft"
                     assertFailure "unbound non-existing binding"
             ]

stressTests = test [ "manyFailures" ~: TestCase $ do
                       rs <- replicateM 100 newEmptyMVar
                       forM (zip rs [0..])
                            (\(res, i) -> forkIO $ withConnection $ \conn ->
                               if i `mod` 2 == 0
                               then do
                                 handle (\(ChannelClosedException _) -> return ()) $
                                   bindQueue conn "meh" "amq.direct" "" >>
                                   putMVar res (assertFailure "unbound meh")
                                 putMVar res (return ())
                               else do
                                 handle (\(e :: SomeException) ->
                                             putMVar res (throw e)) $ do
                                   queue <- declareAnonQueue conn
                                   deleteQueue conn queue
                                   putMVar res (return ()))
                       sequence =<< mapM takeMVar rs
                       return ()
                   ]

openDefaultConnection :: IO Connection
openDefaultConnection = openConnection "localhost" 5672 "/" "guest" "guest"

withConnection :: (Connection -> IO ()) -> IO ()
withConnection = bracket openDefaultConnection closeConnectionNormal
