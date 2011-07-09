module Network.AMQP.Channel (
        Channel,
        openChannel
    ) where

import Network.AMQP.Types ( Channel (..), Connection )

-- | opens a new channel on the connection     
--
-- There's currently no closeChannel method, but you can always just close the connection (the maximum number of channels is 65535).
openChannel :: Connection -> IO Channel
openChannel c = do
    newInQueue <- newChan
    outRes <- newChan
    myLastConsumerTag <- newMVar 0
    ca <- newLock

    myChanClosed <- newMVar Nothing
    myConsumers <- newMVar M.empty 
    
    --get a new unused channelID
    newChannelID <- modifyMVar (lastChannelID c) $ \x -> return (x+1,x+1)
    
    let newChannel = Channel c newInQueue outRes (fromIntegral newChannelID) myLastConsumerTag ca myChanClosed myConsumers


    thrID <- forkIO $ CE.finally (channelReceiver newChannel)
        (closeChannel' newChannel)

    --add new channel to connection's channel map
    modifyMVar_ (connChannels c) (\oldMap -> return $ IM.insert newChannelID (newChannel, thrID) oldMap)
     
    (SimpleMethod (Channel_open_ok _)) <- request newChannel (SimpleMethod (Channel_open (ShortString "")))
    return newChannel        
