/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.zookeeper;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.com.Client.ConnectionLostHandler;
import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Contains basic functionality for a ZooKeeper manager, f.ex. how to get
 * the current master in the cluster.
 */
public abstract class AbstractZooKeeperManager
{
    protected static final String HA_SERVERS_CHILD = "ha-servers";

    private final String servers;
    private final Map<Integer, String> haServersCache = Collections.synchronizedMap(
            new HashMap<Integer, String>() );
    protected volatile Pair<Master, Machine> cachedMaster = NO_MASTER_MACHINE_PAIR;

    protected final StringLogger msgLog;
    protected final int maxConcurrentChannelsPerSlave;
    protected final int clientReadTimeout;
    protected final int clientLockReadTimeout;
    private final long sessionTimeout;

    private final StoreIdGetter storeIdGetter;

    public AbstractZooKeeperManager( String servers, StoreIdGetter storeIdGetter, StringLogger msgLog,
            int clientReadTimeout, int clientLockReadTimeout, int maxConcurrentChannelsPerSlave, int sessionTimeout )
    {
        assert msgLog != null;

        this.servers = servers;
        this.storeIdGetter = storeIdGetter;
        this.msgLog = msgLog;
        this.clientLockReadTimeout = clientLockReadTimeout;
        this.maxConcurrentChannelsPerSlave = maxConcurrentChannelsPerSlave;
        this.clientReadTimeout = clientReadTimeout;
        this.sessionTimeout = sessionTimeout;
    }

    protected String asRootPath( StoreId storeId )
    {
        return "/" + storeId.getCreationTime() + "_" + storeId.getRandomId();
    }

    protected StoreId getClusterStoreId( ZooKeeper keeper, String clusterName )
    {
        try
        {
            byte[] child = keeper.getData( "/" + clusterName, false, null );
            return StoreId.deserialize( child );
        }
        catch ( KeeperException e )
        {
            if ( e.code() == KeeperException.Code.NONODE ) return null;
            throw new ZooKeeperException( "Error getting store id", e );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "Interrupted", e );
        }
    }

    /*
     * Returns int because the ZooKeeper constructor expects an integer,
     * but we are sane and manipulate time as longs.
     */
    protected int getSessionTimeout()
    {
        return (int) sessionTimeout;
    }

    public abstract ZooKeeper getZooKeeper( boolean sync );

    public abstract String getRoot();

    protected Pair<Integer, Integer> parseChild( String child )
    {
        int index = child.indexOf( '_' );
        if ( index == -1 )
        {
            return null;
        }
        int id = Integer.parseInt( child.substring( 0, index ) );
        int seq = Integer.parseInt( child.substring( index + 1 ) );
        return Pair.of( id, seq );
    }

    protected Pair<Long, Integer> readDataRepresentingInstance( String path ) throws InterruptedException, KeeperException
    {
        byte[] data = getZooKeeper( false ).getData( path, false, null );
        ByteBuffer buf = ByteBuffer.wrap( data );
        return Pair.of( buf.getLong(), buf.getInt() );
    }

    protected void invalidateMaster()
    {
        if ( cachedMaster != null )
        {
            Master client = cachedMaster.first();
            if ( client != null )
            {
                client.shutdown();
            }
            cachedMaster = NO_MASTER_MACHINE_PAIR;
        }
    }

    /**
     * Tries to discover the master from the zookeeper information. Will return
     * a {@link Pair} of a {@link Master} and the {@link Machine} it resides
     * on. If the new master is different than the current then the current is
     * invalidated and if allowChange is set to true then the a connection to
     * the new master is established otherwise a NO_MASTER is returned.
     *
     * @param wait Whether to wait for a sync connected event
     * @param allowChange If to connect to the new master
     * @return The master machine pair, possibly a NO_MASTER_MACHINE_PAIR
     */
    protected Pair<Master, Machine> getMasterFromZooKeeper( boolean wait, boolean allowChange )
    {
        return getMasterFromZooKeeper( wait, WaitMode.SESSION, allowChange );
    }

    protected Pair<Master, Machine> bootstrap()
    {
        return getMasterFromZooKeeper( true, WaitMode.STARTUP, true );
    }

    private Pair<Master, Machine> getMasterFromZooKeeper( boolean wait, WaitMode mode, boolean allowChange )
    {
        ZooKeeperMachine master = getMasterBasedOn( getAllMachines( wait, mode ).values() );
        Master masterClient = NO_MASTER;
        if ( cachedMaster.other().getMachineId() != master.getMachineId() )
        {
            invalidateMaster();
            if ( !allowChange ) return NO_MASTER_MACHINE_PAIR;
            if ( master != Machine.NO_MACHINE && master.getMachineId() != getMyMachineId() )
            {
                // If there is a master and it is not me
                masterClient = getMasterClientToMachine( master );
            }
            cachedMaster = Pair.<Master, Machine>of( masterClient,
                    (Machine) master );
        }
        return cachedMaster;
    }

    protected Master getMasterClientToMachine( Machine master )
    {
        if ( master == Machine.NO_MACHINE || master.getServer() == null )
        {
            return NO_MASTER;
        }
        return new MasterClient( master.getServer().first(),
                master.getServer().other(), this.msgLog, storeIdGetter,
                ConnectionLostHandler.NO_ACTION, clientReadTimeout,
                clientLockReadTimeout, maxConcurrentChannelsPerSlave );
    }

    protected abstract int getMyMachineId();

    public Pair<Master, Machine> getCachedMaster()
    {
        return cachedMaster;
    }

    protected ZooKeeperMachine getMasterBasedOn(
            Collection<ZooKeeperMachine> machines )
    {
        ZooKeeperMachine master = null;
        int lowestSeq = Integer.MAX_VALUE;
        long highestTxId = -1;
        for ( ZooKeeperMachine info : machines )
        {
            if ( info.getLastCommittedTxId() != -1 && info.getLastCommittedTxId() >= highestTxId )
            {
                if ( info.getLastCommittedTxId() > highestTxId
                        || info.wasCommittingMaster()
                        || (!master.wasCommittingMaster() && info.getSequenceId() < lowestSeq ) )
                {
                    master = info;
                    lowestSeq = info.getSequenceId();
                    highestTxId = info.getLastCommittedTxId();
                }
            }
        }
        log( "getMaster " + (master != null ? master.getMachineId() : "none") +
                " based on " + machines );
        if ( master != null )
        {
            try
            {
                getZooKeeper( false ).getData(
                        getRoot() + "/" + master.getZooKeeperPath(), true, null );
            }
            catch ( KeeperException e )
            {
                throw new ZooKeeperException(
                        "Unable to get master data while setting watch", e );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new ZooKeeperException(
                        "Interrupted while setting watch on master.", e );
            }
            return master;
        }
        else
        {
            return ZooKeeperMachine.NO_MACHINE;
        }
    }

    protected Map<Integer, ZooKeeperMachine> getAllMachines( boolean wait )
    {
        return getAllMachines( wait, WaitMode.SESSION );
    }

    protected Map<Integer, ZooKeeperMachine> getAllMachines( boolean wait, WaitMode mode )
    {
        if ( wait )
        {
            waitForSyncConnected( mode );
        }
        try
        {
            Map<Integer, ZooKeeperMachine> result = new HashMap<Integer, ZooKeeperMachine>();
            String root = getRoot();
            List<String> children = getZooKeeper( true ).getChildren( root, false );
            for ( String child : children )
            {
                Pair<Integer, Integer> parsedChild = parseChild( child );
                if ( parsedChild == null )
                {
                    continue;
                }

                try
                {
                    int id = parsedChild.first();
                    int seq = parsedChild.other();
                    Pair<Long, Integer> instanceData = readDataRepresentingInstance( root + "/" + child );
                    if ( !result.containsKey( id ) || seq > result.get( id ).getSequenceId() )
                    {
                        result.put( id, new ZooKeeperMachine( id, seq,
                                instanceData.first(), instanceData.other(),
                                getHaServer( id, wait ), HA_SERVERS_CHILD + "/"
                                                         + id ) );
                    }
                }
                catch ( KeeperException inner )
                {
                    if ( inner.code() != KeeperException.Code.NONODE )
                    {
                        throw new ZooKeeperException( "Unable to get master.", inner );
                    }
                }
            }
            return result;
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Unable to get master", e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted.", e );
        }
    }

    protected String getHaServer( int machineId, boolean wait )
    {
        String result = haServersCache.get( machineId );
        if ( result == null )
        {
            result = readHaServer( machineId, wait ).first();
            haServersCache.put( machineId, result );
        }
        return result;
    }

    protected Pair<String /*Host and port*/, Integer /*backup port*/> readHaServer( int machineId, boolean wait )
    {
        if ( wait )
        {
            waitForSyncConnected();
        }
        String rootPath = getRoot();
        String haServerPath = rootPath + "/" + HA_SERVERS_CHILD + "/" + machineId;
        try
        {
            byte[] serverData = getZooKeeper( true ).getData( haServerPath, false, null );
            ByteBuffer buffer = ByteBuffer.wrap( serverData );
            int backupPort = buffer.getInt();
            byte length = buffer.get();
            char[] chars = new char[length];
            buffer.asCharBuffer().get( chars );
            String result = String.valueOf( chars );
            log( "Read HA server:" + result + " (for machineID " + machineId +
                    ") from zoo keeper" );
            return Pair.of( result, backupPort );
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Couldn't find the HA server: " + rootPath, e );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "Interrupted", e );
        }
    }

    private void log( String string )
    {
        if ( msgLog != null )
        {
            msgLog.logMessage( string );
        }
    }

    public void shutdown()
    {
        try
        {
            invalidateMaster();
            cachedMaster = NO_MASTER_MACHINE_PAIR;
            getZooKeeper( false ).close();
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException(
                "Error closing zookeeper connection", e );
        }
    }

    public final void waitForSyncConnected()
    {
        waitForSyncConnected( WaitMode.SESSION );
    }

    abstract void waitForSyncConnected( WaitMode mode );

    enum WaitMode
    {
        STARTUP
        {
            @Override
            public WaitStrategy getStrategy( AbstractZooKeeperManager zooClient )
            {
                return new StartupWaitStrategy( zooClient.msgLog );
            }
        },
        SESSION
        {
            @Override
            public WaitStrategy getStrategy( AbstractZooKeeperManager zooClient )
            {
                return new SessionWaitStrategy( zooClient.getSessionTimeout() );
            }
        };

        public abstract WaitStrategy getStrategy( AbstractZooKeeperManager zooClient );
    }

    interface WaitStrategy
    {
        abstract boolean waitMore( long waitedSoFar );
    }

    private static class SessionWaitStrategy implements WaitStrategy
    {
        private final long sessionTimeout;

        SessionWaitStrategy( long sessionTimeout )
        {
            this.sessionTimeout = sessionTimeout;
        }

        @Override
        public boolean waitMore( long waitedSoFar )
        {
            return waitedSoFar < sessionTimeout;
        }
    }

    private static class StartupWaitStrategy implements WaitStrategy
    {
        static final long SECONDS_TO_WAIT_BETWEEN_NOTIFICATIONS = 30;

        private long lastNotification = 0;
        private final StringLogger msgLog;

        public StartupWaitStrategy( StringLogger msgLog )
        {
            this.msgLog = msgLog;
        }

        @Override
        public boolean waitMore( long waitedSoFar )
        {
            long currentNotification = waitedSoFar / ( SECONDS_TO_WAIT_BETWEEN_NOTIFICATIONS * 1000 );
            if ( currentNotification > lastNotification )
            {
                lastNotification = currentNotification;
                msgLog.logMessage( "Have been waiting for " + SECONDS_TO_WAIT_BETWEEN_NOTIFICATIONS
                                   * currentNotification + " seconds for the ZooKeeper cluster to respond." );
            }
            return true;
        }
    }

    public String getServers()
    {
        return servers;
    }

    protected static final Master NO_MASTER = new Master()
    {
        @Override
        public void shutdown() {}

        @Override
        public Response<Void> pullUpdates( SlaveContext context )
        {
            throw noMasterException();
        }

        private ComException noMasterException()
        {
            return new NoMasterException();
        }

        @Override
        public Response<Void> initializeTx( SlaveContext context )
        {
            throw noMasterException();
        }

        @Override
        public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( long txId, StoreId myStoreId )
        {
            throw noMasterException();
        }

        @Override
        public Response<Void> finishTransaction( SlaveContext context, boolean success )
        {
            throw noMasterException();
        }

        @Override
        public Response<Integer> createRelationshipType( SlaveContext context, String name )
        {
            throw noMasterException();
        }

        @Override
        public Response<Void> copyStore( SlaveContext context, StoreWriter writer )
        {
            throw noMasterException();
        }

        @Override
        public Response<Void> copyTransactions( SlaveContext context,
                String dsName, long startTxId, long endTxId )
        {
            throw noMasterException();
        }

        @Override
        public Response<Long> commitSingleResourceTransaction( SlaveContext context, String resource,
                TxExtractor txGetter )
        {
            throw noMasterException();
        }

        @Override
        public Response<IdAllocation> allocateIds( IdType idType )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireRelationshipWriteLock( SlaveContext context,
                long... relationships )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireRelationshipReadLock( SlaveContext context,
                long... relationships )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireNodeWriteLock( SlaveContext context, long... nodes )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireNodeReadLock( SlaveContext context, long... nodes )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireGraphWriteLock( SlaveContext context )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireGraphReadLock( SlaveContext context )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireIndexReadLock( SlaveContext context, String index, String key )
        {
            throw noMasterException();
        }

        @Override
        public Response<LockResult> acquireIndexWriteLock( SlaveContext context, String index, String key )
        {
            throw noMasterException();
        }

        @Override
        public String toString()
        {
            return "NO_MASTER";
        }
    };

    public static final Pair<Master, Machine> NO_MASTER_MACHINE_PAIR = Pair.of(
            NO_MASTER, (Machine) ZooKeeperMachine.NO_MACHINE );
}
