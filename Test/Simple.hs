{-# LANGUAGE ScopedTypeVariables, OverloadedStrings #-}

import Control.Concurrent ( ThreadId, forkIO, killThread )
import Control.Concurrent.MVar ( newEmptyMVar, putMVar, takeMVar, tryPutMVar )
import qualified Control.Exception as CE
import Control.Monad ( forM_, replicateM )
import Control.Monad.IO.Class ( MonadIO(..) )
import Network.AMQP
import Network.AMQP.Types ( AMQPException(..) )
import System.Exit ( exitFailure )
import System.Posix.Unistd ( sleep )
import Test.HUnit

main :: IO ()
main = do
  runCounts <- runTestTT $ test [tests, stressTests]
  if failures runCounts + errors runCounts == 0
     then
       putStrLn "All tests pass :)"
     else do
       putStrLn "Failures or errors occured :'("
       exitFailure

tests :: Test
tests = test [ "alwaysPass" ~: TestCase $
                 return ()
             , "connectionOpenClose" ~: TestCase $
                 closeConnectionNormal =<< openDefaultConnection
             , "connectionOpenClose10" ~: TestCase $ do
                 conns <- replicateM 10 openDefaultConnection
                 mapM_ closeConnectionNormal conns
             , "connectionNoServer" ~: TestCase $
                 CE.handle (\(_ :: CE.IOException) -> return ()) $ do
                     openConnection "localhost" (5600 :: Int) "/" "guest" "guest"
                     assertFailure "connected to non-existing broker"
             , "connectionWrongLogin" ~: TestCase $
                 CE.handle (\(ConnectionClosedException _) -> return ()) $ do
                     openConnection "localhost" 5672 "/" "guest" "geust"
                     assertFailure "connected with wrong password"
             , "connectionCloseHandler" ~: TestCase $ do
                 conn <- openDefaultConnection
                 m <- newEmptyMVar
                 addConnectionClosedHandler conn (putMVar m . Right)
                 closeConnectionNormal conn
                 tid <- timeout 1 $ tryPutMVar m (Left "timeout")
                 v <- takeMVar m
                 case v of
                   Left err ->
                       assertFailure err
                   Right (ConnectionClosedException "Normal") ->
                       killThread tid
             , "queueDeclare" ~: TestCase $
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   return ()
             , "queueDoubleDeclare" ~: TestCase $
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   declareQueue conn "test-queue"
                   return ()
             , "queueDeclareDelete" ~: TestCase $
                 withConnection simpleQueueOp
             , "queueDelete" ~: TestCase $
                 withConnection $ \conn ->
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteQueue conn "test-queue"
                     assertFailure "deleted non-existing queue"
             , "queueDeleteDeclare" ~: TestCase $
                 withConnection $ \conn -> do
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteQueue conn "test-queue"
                     return ()
                   simpleQueueOp conn
             , "exchangeDeclare" ~: TestCase $
                 withConnection $ \conn -> do
                   declareExchange conn "test-exchange" "direct" False
                   return ()
             , "exchangeDeclareDelete" ~: TestCase $
                 withConnection simpleExchgOp
             , "exchangeDelete" ~: TestCase $
                 withConnection $ \conn ->
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteExchange conn "test-exchange"
                     assertFailure "deleted non-existing exchange"
             , "exchangeDeleteDeclare" ~: TestCase $
                 withConnection $ \conn -> do
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     deleteExchange conn "test-exchange"
                     return ()
                   simpleExchgOp conn
             , "queueBindUnbind" ~: TestCase $
                 withConnection $ \conn -> do
                   declareQueue conn "test-queue"
                   (do
                     bindQueue conn "test-queue" "amq.direct" ""
                     unbindQueue conn "test-queue" "amq.direct" "")
                    `CE.finally`
                      deleteQueue conn "test-queue"
             , "queueUnbindNonExisting" ~: TestCase $
                 withConnection $ \conn ->
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     unbindQueue conn "test-queue" "amq.direct" ""
                     assertFailure "unbound non-existing queue"
             , "queueUnbindNonExisting2" ~: TestCase $
                 withConnection $ \conn ->
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     declareQueue conn "test-queue"
                     (do
                       unbindQueue conn "test-queue" "amq.direct" ""
                       assertFailure "unbound non-existing binding")
                      `CE.finally`
                        deleteQueue conn "test-queue"
             , "exchangeBindUnbind" ~: TestCase $
                 withConnection $ \conn -> do
                   bindExchange conn "amq.fanout" "amq.direct" ""
                   unbindExchange conn "amq.fanout" "amq.direct" ""
             , "exchangeUnbindNonExisting" ~: TestCase $
                 withConnection $ \conn ->
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     unbindExchange conn "no-such-exchange" "amq.direct" ""
                     assertFailure "unbound non-existing exchange"
             , "exchangeUnbindNonExisting2" ~: TestCase $
                 withConnection $ \conn ->
                   CE.handle (\(ChannelClosedException _) -> return ()) $ do
                     unbindExchange conn "amq.fanout" "amq.direct" "pfft"
                     assertFailure "unbound non-existing binding"
             , "justPublish" ~: TestCase $
                 withConnection $ \conn -> do
                     runPublisher conn $ publish "" "bah" "meh" >> return ()
                     return ()
             , "justPublish2" ~: TestCase $
                 withConnection $ \conn -> do
                     waiter <- newEmptyMVar
                     runPublisherBracket conn
                                         (return ())
                                         (\_ -> putMVar waiter (return ()))
                                         (\(e :: CE.SomeException) ->
                                              putMVar waiter (CE.throw e)) $ \_ -> do
                       publish "" "bah" "meh" >> return ()
                     act <- takeMVar waiter
                     act
             , "justPublishBad" ~: TestCase $
                 withConnection $ \conn -> do
                     waiter <- newEmptyMVar
                     runPublisherBracket conn
                                         (return ())
                                         (\_ -> return ())
                                         (\(e :: CE.SomeException) ->
                                              putMVar waiter (Left e)) $ \_ -> do
                       publish "ni" "bah" "meh"
                       liftIO $ sleep 1 >> putMVar waiter (Right ())
                     res <- takeMVar waiter
                     case res of
                       Left _  -> return ()
                       Right () -> assertFailure "succesfully published to \
                                                \non-existing exchange"
             ]

stressTests :: Test
stressTests = test [ "manyFailures" ~: TestCase $ do
                       rs <- replicateM 100 newEmptyMVar
                       forM_ (zip rs [(0 :: Int)..])
                            (\(res, i) -> forkIO $ withConnection $ \conn ->
                               if i `mod` 2 == 0
                               then do
                                 CE.handle (\(ChannelClosedException _) ->
                                                return ()) $
                                   bindQueue conn "meh" "amq.direct" "" >>
                                   putMVar res (assertFailure "unbound meh")
                                 putMVar res (return ())
                               else
                                 CE.handle (\(e :: CE.SomeException) ->
                                             putMVar res (CE.throw e)) $ do
                                   queue <- declareAnonQueue conn
                                   deleteQueue conn queue
                                   putMVar res (return ()))
                       sequence =<< mapM takeMVar rs
                       return ()
                   ]

openDefaultConnection :: IO Connection
openDefaultConnection = openConnection "localhost" (5672 :: Int) "/" "guest" "guest"

withConnection :: (Connection -> IO ()) -> IO ()
withConnection = CE.bracket openDefaultConnection closeConnectionNormal

simpleQueueOp :: Connection -> IO ()
simpleQueueOp conn = do
  declareQueue conn "test-queue"
  deleteQueue conn "test-queue"
  return ()

simpleExchgOp :: Connection -> IO ()
simpleExchgOp conn = do
  declareExchange conn "test-exchange" "direct" False
  deleteExchange conn "test-exchange"
  return ()

timeout :: Int -> IO a -> IO ThreadId
timeout interval act = forkIO $ do
                         sleep interval
                         act
                         return ()
