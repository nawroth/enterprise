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
package org.neo4j.kernel;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.transaction.TransactionManager;

import org.neo4j.com.Client;
import org.neo4j.com.ComException;
import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.EnterpriseConfigurationMigrator;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterGraphDatabase;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.ha.SlaveGraphDatabase;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;

public class HighlyAvailableGraphDatabase
        implements GraphDatabaseService, GraphDatabaseSPI
{
    private static final int STORE_COPY_RETRIES = 3;

    @ConfigurationPrefix( "ha." )
    public interface Configuration
        extends AbstractGraphDatabase.Configuration
    {

        int read_timeout( int defaultReadResponseTimeoutSeconds );

        SlaveUpdateMode slave_coordinator_update_mode( SlaveUpdateMode def );

        int server_id();

        BranchedDataPolicy branched_data_policy( BranchedDataPolicy def );
    }
    

    protected final int localGraphWait;
    protected StoreId storeId;
    protected final StoreIdGetter storeIdGetter;

    protected Configuration configuration;
    private String storeDir;
    private final StringLogger messageLog;
    private Map<String, String> config;
    private AbstractGraphDatabase internalGraphDatabase;
    private NodeProxy.NodeLookup nodeLookup;
    private RelationshipProxy.RelationshipLookups relationshipLookups;

    private ResponseReceiver responseReceiver;
    private volatile Broker broker;
    private int machineId;
    private volatile MasterServer masterServer;
    private ScheduledExecutorService updatePuller;
    private volatile long updateTime = 0;
    private volatile Throwable causeOfShutdown;
    private long startupTime;
    private BranchedDataPolicy branchedDataPolicy;
    private SlaveUpdateMode slaveUpdateMode;
    private int readTimeout;

    // This lock is used to safeguard access to internal database
    // Users will acquire readlock, and upon master/slave switch
    // a write lock will be acquired
    private ReadWriteLock databaseLock;

    /*
     *  True iff it is ok to pull updates. Used to control the
     *  update puller during master switches, to reduce could not connect
     *  log statements. More elegant that stopping and starting the executor.
     */
    private volatile boolean pullUpdates;

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();

    /**
     * Will instantiate its own ZooKeeper broker
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config )
    {
        this.storeDir = storeDir;
        messageLog = StringLogger.logger( this.storeDir );

        this.config = new EnterpriseConfigurationMigrator(messageLog).migrateConfiguration( config );
        

        responseReceiver = new HAResponseReceiver();
        
        databaseLock = new ReentrantReadWriteLock(  );
        
        this.nodeLookup = new HANodeLookup();
        
        this.relationshipLookups = new HARelationshipLookups();

        this.config.put( Config.KEEP_LOGICAL_LOGS, "true" );

        configuration = ConfigProxy.config( config, Configuration.class );

        this.startupTime = System.currentTimeMillis();
        kernelEventHandlers.add( new TxManagerCheckKernelEventHandler() );

        this.readTimeout = configuration.read_timeout( Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS );
        this.slaveUpdateMode = configuration.slave_coordinator_update_mode( SlaveUpdateMode.async );
        this.machineId = configuration.server_id();
        this.branchedDataPolicy = configuration.branched_data_policy( HighlyAvailableGraphDatabase.BranchedDataPolicy.keep_all );
        this.localGraphWait = Math.max( configuration.read_timeout( 15 ), 5 );

        storeIdGetter = new StoreIdGetter()
        {
            @Override
            public StoreId get()
            {
                if ( storeId == null ) throw new IllegalStateException( "No store ID" );
                return storeId;
            }
        };

        // TODO The dependency from BrokerFactory to 'this' is completely broken. Needs rethinking
        this.broker = createBroker();
        this.pullUpdates = false;

        start();
    }

    // GraphDatabaseService implementation
    // TODO This pattern is broken. Should lock database for duration of call, not just on access of db
    @Override
    public Node createNode()
    {
        return localGraph().createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        return localGraph().getNodeById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return localGraph().getReferenceNode();
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return localGraph().getAllNodes();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return localGraph().getRelationshipTypes();
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return localGraph().getRelationshipById( id );
    }

    @Override
    public IndexManager index()
    {
        return localGraph().index();
    }

    @Override
    public Transaction beginTx()
    {
        return localGraph().beginTx();
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown( new IllegalStateException( "shutdown called" ), true );
    }

    // GraphDatabaseSPI implementation

    @Override
    public NodeManager getNodeManager()
    {
        return localGraph().getNodeManager();
    }

    @Override
    public LockReleaser getLockReleaser()
    {
        return localGraph().getLockReleaser();
    }

    @Override
    public LockManager getLockManager()
    {
        return localGraph().getLockManager();
    }

    @Override
    public XaDataSourceManager getXaDataSourceManager()
    {
        return localGraph().getXaDataSourceManager();
    }

    @Override
    public TransactionManager getTxManager()
    {
        return localGraph().getTxManager();
    }

    @Override
    public DiagnosticsManager getDiagnosticsManager()
    {
        return localGraph().getDiagnosticsManager();
    }

    @Override
    public StringLogger getMessageLog()
    {
        return messageLog;
    }

    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return localGraph().getRelationshipTypeHolder();
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return localGraph().getIdGeneratorFactory();
    }

    public StoreIdGetter getStoreIdGetter()
    {
        return storeIdGetter;
    }

    @Override
    public KernelData getKernelData()
    {
        return localGraph().getKernelData();
    }

    public <T> T getSingleManagementBean( Class<T> type )
    {
        return localGraph().getSingleManagementBean( type );
    }

    // Internal
    private void getFreshDatabaseFromMaster( Pair<Master, Machine> master,
                boolean branched )
    {
        assert master != null;
        // Assume it's shut down at this point
        internalShutdown( false );
        if ( branched )
        {
            makeWayForNewDb();
        }

        Exception exception = null;
        for ( int i = 0; i < STORE_COPY_RETRIES; i++ )
        {
            try
            {
                copyStoreFromMaster( master );
                return;
            }
            // TODO Maybe catch IOException and treat it more seriously?
            catch ( Exception e )
            {
                getMessageLog().logMessage( "Problems copying store from master", e );
                sleepWithoutInterruption( 1000, "" );
                exception = e;
                master = broker.getMasterReally( true );
                BranchedDataPolicy.keep_none.handle( this );
            }
        }
        throw new RuntimeException( "Gave up trying to copy store from master", exception );
    }

    void makeWayForNewDb()
    {
        this.messageLog.logMessage( "Cleaning database " + storeDir + " (" + branchedDataPolicy.name() +
                                         ") to make way for new db from master" );
        branchedDataPolicy.handle( this );
    }

    protected void start()
    {
        getMessageLog().logMessage( "Starting up highly available graph database '" + getStoreDir() + "'" );

        StoreId storeId = null;
        if ( !new File( storeDir, NeoStore.DEFAULT_NAME ).exists() )
        {   // Try for
            long endTime = System.currentTimeMillis()+60000;
            Exception exception = null;
            while ( System.currentTimeMillis() < endTime )
            {
                // Check if the cluster is up
                Pair<Master, Machine> master = broker.getMasterReally( true );
                if ( master != null && !master.other().equals( Machine.NO_MACHINE ) &&
                        master.other().getMachineId() != machineId )
                {   // Join the existing cluster
                    try
                    {
                        getFreshDatabaseFromMaster( master, false /*branched*/);
                        messageLog.logMessage( "copied store from master" );
                        exception = null;
                        break;
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                        master = broker.getMasterReally( true );
                        messageLog.logMessage( "Problems copying store from master", e );
                    }
                }
                else
                {   // I seem to be the master, the broker have created the cluster for me
                    // I'm just going to start up now
//                    storeId = broker.getClusterStoreId();
                    break;
                }
                // I am not master, and could not connect to the master:
                // wait for other machine(s) to join.
                sleepWithoutInterruption( 300, "Startup interrupted" );
            }

            if ( exception != null )
            {
                throw new RuntimeException( "Tried to join the cluster, but was unable to", exception );
            }
        }
        storeId = broker.getClusterStoreId();
        System.out.println( "Start up storeId:" + storeId );
        newMaster( storeId, new Exception( "Starting up for the first time" ) );
        localGraph();
    }

    private void sleepWithoutInterruption( long time, String errorMessage )
    {
        try
        {
            Thread.sleep( time );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( errorMessage, e );
        }
    }

    private void copyStoreFromMaster( Pair<Master, Machine> master ) throws Exception
    {
        messageLog.logMessage( "Copying store from master" );
        Response<Void> response = master.first().copyStore( new SlaveContext( 0, machineId, 0, new Pair[0] ),
                new ToFileStoreWriter( storeDir ) );
        long highestLogVersion = highestLogVersion();
        if ( highestLogVersion > -1 ) NeoStore.setVersion( storeDir, highestLogVersion + 1 );
        EmbeddedGraphDatabase copiedDb = new EmbeddedGraphDatabase( storeDir, stringMap( KEEP_LOGICAL_LOGS, "true" ) );
        try
        {
            MasterUtil.applyReceivedTransactions( response, copiedDb, MasterUtil.txHandlerForFullCopy() );
        }
        finally
        {
            copiedDb.shutdown();
        }
        messageLog.logMessage( "Done copying store from master" );
    }

    private long highestLogVersion()
    {
        return XaLogicalLog.getHighestHistoryLogVersion( new File( storeDir ), LOGICAL_LOG_DEFAULT_NAME );
    }

    /**
     * Access to the internal database reference. Uses a read-lock to ensure that we are not currently restarting it
     *
     * @return
     */
    private AbstractGraphDatabase localGraph()
    {
        try
        {
            if (databaseLock.readLock().tryLock( localGraphWait, TimeUnit.SECONDS ))
            {
                try
                {
                    if (internalGraphDatabase == null)
                        if ( causeOfShutdown != null )
                        {
                            throw new RuntimeException( "Graph database not started", causeOfShutdown );
                        }
                        else
                        {
                            throw new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                                    "maybe not started yet or in the middle of master/slave swap?" );
                        }

                    return internalGraphDatabase;
                } finally
                {
                    databaseLock.readLock().unlock();
                }
            } else
            {
                if ( causeOfShutdown != null )
                {
                    throw new RuntimeException( "Graph database not started", causeOfShutdown );
                }
                else
                {
                    throw new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                            "maybe not started yet or in the middle of master/slave swap?" );
                }
            }
        }
        catch( InterruptedException e )
        {
            if ( causeOfShutdown != null )
            {
                throw new RuntimeException( "Graph database not started", causeOfShutdown );
            }
            else
            {
                throw new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                        "maybe not started yet or in the middle of master/slave swap?" );
            }
        }

/* This code was not thread-safe. At all.

        if ( graphDbImpl != null ) return graphDbImpl;
        int secondsWait = Math.max( configuration.read_timeout( 15 ), 5);

        long endTime = System.currentTimeMillis()+secondsWait*1000;
        while ( graphDbImpl == null && System.currentTimeMillis() < endTime )
        {
            sleepWithoutInterruption( 1, "Failed waiting for local graph to be available" );
            if ( graphDbImpl != null ) return graphDbImpl;
        }

        if ( causeOfShutdown != null )
        {
            throw new RuntimeException( "Graph database not started", causeOfShutdown );
        }
        else
        {
            throw new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                    "maybe not started yet or in the middle of master/slave swap?" );
        }*/
    }

    public Broker getBroker()
    {
        return this.broker;
    }

    public void pullUpdates()
    {
        try
        {
            if ( masterServer == null )
            {
                if ( broker.getMaster() == null
                     && broker instanceof ZooKeeperBroker )
                {
                    /*
                     * Log a message - the ZooKeeperBroker should not return
                     * null master
                     */
                    messageLog.logMessage(
                            "ZooKeeper broker returned null master" );
                    newMaster( null, new NullPointerException(
                            "master returned from broker" ) );
                }
                else if ( broker.getMaster().first() == null )
                {
                    newMaster( null, new NullPointerException(
                            "master returned from broker" ) );
                }
                responseReceiver.receive( broker.getMaster().first().pullUpdates(
                    responseReceiver.getSlaveContext( -1 ) ) );
            }
        }
        catch ( ZooKeeperException e )
        {
            newMaster( null, e );
            throw e;
        }
        catch ( ComException e )
        {
            newMaster( null, e );
            throw e;
        }
    }

    private void updateTime()
    {
        this.updateTime = System.currentTimeMillis();
    }

    long lastUpdateTime()
    {
        return this.updateTime;
    }

    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return localGraph().getManagementBeans( type );
    }

    public final <T> T getManagementBean( Class<T> type )
    {
        return localGraph().getManagementBean( type );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + storeDir + ", " + HaConfig.CONFIG_KEY_SERVER_ID + ":" + machineId + "]";
    }

    protected synchronized void reevaluateMyself( StoreId storeId )
    {
        Pair<Master, Machine> master = broker.getMasterReally( true );
        boolean iAmCurrentlyMaster = masterServer != null;
        messageLog.logMessage( "ReevaluateMyself: machineId=" + machineId + " with master[" + master +
                                    "] (I am master=" + iAmCurrentlyMaster + ", " + internalGraphDatabase + ")" );
        pullUpdates = false;
        AbstractGraphDatabase newDb = null;
        try
        {
            if ( master.other().getMachineId() == machineId )
            {   // I am the new master
                if ( this.internalGraphDatabase == null || !iAmCurrentlyMaster )
                {   // I am currently a slave, so restart as master
                    internalShutdown( true );
                    newDb = startAsMaster( storeId );
                }
                // fire rebound event
                broker.rebindMaster();
            }
            else
            {   // Someone else is master
                broker.notifyMasterChange( master.other() );
                if ( this.internalGraphDatabase == null || iAmCurrentlyMaster )
                {   // I am currently master, so restart as slave.
                    // This will result in clearing of free ids from .id files, see SlaveIdGenerator.
                    internalShutdown( true );
                    newDb = startAsSlave(storeId);
                }
                else
                {   // I am already a slave, so just forget the ids I got from the previous master
                    SlaveGraphDatabase slave = (SlaveGraphDatabase) internalGraphDatabase;
                    slave.forgetIdAllocationsFromMaster();

                }

                ensureDataConsistencyWithMaster( newDb != null ? newDb : internalGraphDatabase, master );
                messageLog.logMessage( "Data consistent with master" );
            }
            if ( newDb != null )
            {
                doAfterLocalGraphStarted( newDb );

                // Assign the db last
                this.internalGraphDatabase = newDb;

                /*
                 * We have to instantiate the update puller after the local db has been assigned.
                 * Another way to do it is to wait on a LocalGraphAvailableCondition. I chose this,
                 * it is simpler to follow, provided you know what a volatile does.
                 */
                if ( masterServer == null )
                {
                    // The above being true means we are a slave
                    instantiateAutoUpdatePullerIfConfigSaysSo();
                    pullUpdates = true;
                }
            }
        }
        catch ( Throwable t )
        {
            safelyShutdownDb( newDb );
            throw launderedException( t );
        }
    }

    private void safelyShutdownDb( AbstractGraphDatabase newDb )
    {
        try
        {
            if ( newDb != null ) newDb.shutdown();
        }
        catch ( Exception e )
        {
            messageLog.logMessage( "Couldn't shut down newly started db", e );
        }
    }

    private void doAfterLocalGraphStarted( AbstractGraphDatabase newDb )
    {
        broker.setConnectionInformation( newDb.getKernelData() );
        for ( TransactionEventHandler<?> handler : transactionEventHandlers )
        {
            newDb.registerTransactionEventHandler( handler );
        }
        for ( KernelEventHandler handler : kernelEventHandlers )
        {
            newDb.registerKernelEventHandler( handler );
        }
    }

    private void logHaInfo( String started )
    {
        messageLog.logMessage( started, true );
        messageLog.logMessage( "--- HIGH AVAILABILITY CONFIGURATION START ---" );
        broker.logStatus( messageLog );
        messageLog.logMessage( "--- HIGH AVAILABILITY CONFIGURATION END ---", true );
    }

    private AbstractGraphDatabase startAsSlave( StoreId storeId)
    {
        messageLog.logMessage( "Starting[" + machineId + "] as slave", true );
        this.storeId = storeId;
        SlaveGraphDatabase slaveGraphDatabase = new SlaveGraphDatabase( storeDir, config,
                                                                        this, broker, messageLog, responseReceiver, slaveUpdateMode.createUpdater( broker ),
                                                                        nodeLookup, relationshipLookups);
/*

        EmbeddedGraphDbImpl result = new EmbeddedGraphDbImpl( getStoreDir(), this,
                                                              new SlaveLockManager( txManager, txHook, broker, this ),
                txManager,
                lockReleaser,
                kernelEventHandlers,
                transactionEventHandlers,
                kernelPanicEventGenerator,
                extensions,
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxHook( broker, this ),
                slaveUpdateMode.createUpdater( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
*/
        // instantiateAutoUpdatePullerIfConfigSaysSo() moved to
        // reevaluateMyself(), after the local db has been assigned
        logHaInfo( "Started as slave" );
        return slaveGraphDatabase;
    }

    private AbstractGraphDatabase startAsMaster( StoreId storeId )
    {
        messageLog.logMessage( "Starting[" + machineId + "] as master", true );

        MasterGraphDatabase master = new MasterGraphDatabase( storeDir, config, storeId, this, broker, messageLog, nodeLookup, relationshipLookups);
        
/*
        EmbeddedGraphDbImpl result = new EmbeddedGraphDbImpl( getStoreDir(), storeId, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                new MasterTxIdGeneratorFactory( broker ),
                new MasterTxHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
*/
        this.masterServer = (MasterServer) broker.instantiateMasterServer( master );
        logHaInfo( "Started as master" );
        return master;
    }

    // TODO This should be moved to SlaveGraphDatabase
    private void ensureDataConsistencyWithMaster( AbstractGraphDatabase newDb, Pair<Master, Machine> master )
    {
        if ( master.other().getMachineId() == machineId )
        {
            messageLog.logMessage( "I am master so cannot consistency check data with master" );
            return;
        }
        else if ( master.first() == null )
        {
            // Temporarily disconnected from ZK
            RuntimeException cause = new RuntimeException( "Unable to get master from ZK" );
            shutdown( cause, false );
            throw cause;
        }

        XaDataSource nioneoDataSource = newDb.getXaDataSourceManager().getNeoStoreDataSource();
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        Pair<Integer,Long> myMaster;
        try
        {
            myMaster = nioneoDataSource.getMasterForCommittedTx( myLastCommittedTx );
        }
        catch ( NoSuchLogVersionException e )
        {
            messageLog.logMessage( "Logical log file for txId " + myLastCommittedTx +
                " not found, perhaps due to the db being copied from master. Ignoring." );
            return;
        }
        catch ( IOException e )
        {
            messageLog.logMessage( "Failed to get master ID for txId " + myLastCommittedTx + ".", e );
            return;
        }
        catch ( Exception e )
        {
            getMessageLog().logMessage(
                                "Exception while getting master ID for txId "
                                        + myLastCommittedTx + ".", e );
            throw new BranchedDataException( "Maybe not branched data, but it could solve it", e );
        }

        long endTime = System.currentTimeMillis()+readTimeout*1000;
        Pair<Integer, Long> mastersMaster = null;
        RuntimeException failure = null;
        while ( mastersMaster == null && System.currentTimeMillis() < endTime )
        {
            try
            {
                mastersMaster = master.first().getMasterIdForCommittedTx(
                        myLastCommittedTx, getStoreId( newDb ) ).response();
            }
            catch ( ComException e )
            {   // Maybe new master isn't up yet... let's wait a little and retry
                failure = e;
                sleepWithoutInterruption( 500, "Failed waiting for next attempt to contact master" );
            }
        }
        if ( mastersMaster == null ) throw failure;

        if ( myMaster.first() != XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER
            && !myMaster.equals( mastersMaster ) )
        {
            String msg = "Branched data, I (machineId:" + machineId + ") think machineId for txId (" +
                    myLastCommittedTx + ") is " + myMaster + ", but master (machineId:" +
                    master.other().getMachineId() + ") says that it's " + mastersMaster;
            messageLog.logMessage( msg, true );
            RuntimeException exception = new BranchedDataException( msg );
            safelyShutdownDb( newDb );
            shutdown( exception, false );
            throw exception;
        }
        messageLog.logMessage( "Master id for last committed tx ok with highestTxId=" +
            myLastCommittedTx + " with masterId=" + myMaster, true );
    }

    private StoreId getStoreId( AbstractGraphDatabase db )
    {
        XaDataSource ds = db.getXaDataSourceManager().getNeoStoreDataSource();
        return ((NeoStoreXaDataSource) ds).getStoreId();
    }

    private void instantiateAutoUpdatePullerIfConfigSaysSo()
    {
        long pullInterval = HaConfig.getPullIntervalFromConfig( config );
        if ( pullInterval > 0 )
        {
            updatePuller = new ScheduledThreadPoolExecutor( 1 );
            updatePuller.scheduleWithFixedDelay( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        if ( pullUpdates )
                        {
                            pullUpdates();
                        }
                    }
                    catch ( Exception e )
                    {
                        messageLog.logMessage( "Pull updates failed", e  );
                    }
                }
            }, pullInterval, pullInterval, TimeUnit.MILLISECONDS );
        }
    }

    public TransactionBuilder tx()
    {
        return localGraph().tx();
    }

    public synchronized void internalShutdown( boolean rotateLogs )
    {
        messageLog.logMessage( "Internal shutdown of HA db[" + machineId + "] reference=" + this + ", masterServer=" + masterServer, new Exception( "Internal shutdown" ), true );
        pullUpdates = false;
        if ( this.updatePuller != null )
        {
            messageLog.logMessage( "Internal shutdown updatePuller", true );
            try
            {
                /*
                 * Be gentle, interrupting running threads could leave the
                 * file channels in a bad shape.
                 */
                this.updatePuller.shutdown();
                this.updatePuller.awaitTermination( 5, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                messageLog.logMessage(
                        "Got exception while waiting for update puller termination",
                        e, true );
            }
            messageLog.logMessage( "Internal shutdown updatePuller DONE",
                    true );
            this.updatePuller = null;
        }
        if ( this.masterServer != null )
        {
            messageLog.logMessage( "Internal shutdown masterServer", true );
            this.masterServer.shutdown();
            messageLog.logMessage( "Internal shutdown masterServer DONE", true );
            this.masterServer = null;
        }
        if ( this.internalGraphDatabase != null )
        {
            if ( rotateLogs )
            {
                internalGraphDatabase.getXaDataSourceManager().rotateLogicalLogs();
            }
            messageLog.logMessage( "Internal shutdown localGraph", true );
            this.internalGraphDatabase.shutdown();
            messageLog.logMessage( "Internal shutdown localGraph DONE", true );
            this.internalGraphDatabase = null;
        }
        messageLog.flush();
    }

    private synchronized void shutdown( Throwable cause, boolean shutdownBroker )
    {
        causeOfShutdown = cause;
        messageLog.logMessage( "Shutdown[" + machineId + "], " + this, true );
        if ( shutdownBroker && this.broker != null )
        {
            this.broker.shutdown();
        }
        internalShutdown( false );
    }

    protected synchronized void close()
    {
        shutdown( new IllegalStateException( "shutdown called" ), true );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        this.kernelEventHandlers.add( handler );
        return localGraph().registerKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return localGraph().registerTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        try
        {
            return localGraph().unregisterKernelEventHandler( handler );
        }
        finally
        {
            kernelEventHandlers.remove( handler );
        }
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        try
        {
            return localGraph().unregisterTransactionEventHandler( handler );
        }
        finally
        {
            transactionEventHandlers.remove( handler );
        }
    }

    synchronized void newMaster( StoreId storeId, Exception e )
    {
        databaseLock.writeLock().lock();
        try
        {
            doNewMaster( storeId, e );
        }
        catch ( BranchedDataException bde )
        {
            messageLog.logMessage( "Branched data occured, retrying" );
            getFreshDatabaseFromMaster(broker.getMasterReally( true ), true /*branched*/);
            doNewMaster( storeId, bde );
        } finally
        {
            databaseLock.writeLock().unlock();
        }

    }

    private void doNewMaster( StoreId storeId, Exception e )
    {
        try
        {
            messageLog.logMessage( "newMaster called", true );
            reevaluateMyself( storeId );
        }
        catch ( ZooKeeperException ee )
        {
            messageLog.logMessage( "ZooKeeper exception in newMaster", ee );
            throw Exceptions.launderedException( ee );
        }
        catch ( ComException ee )
        {
            messageLog.logMessage( "Communication exception in newMaster", ee );
            throw Exceptions.launderedException( ee );
        }
        catch ( BranchedDataException ee )
        {
            throw ee;
        }
        // BranchedDataException will escape from this method since the catch clause below
        // sees to that.
        catch ( Throwable t )
        {
            messageLog.logMessage( "Reevaluation ended in unknown exception " + t
                    + " so shutting down", t, true );
            shutdown( t, false );
            throw Exceptions.launderedException( t );
        }
    }

    public MasterServer getMasterServerIfMaster()
    {
        return masterServer;
    }

    protected int getMachineId()
    {
        return machineId;
    }

    public boolean isMaster()
    {
        return broker.iAmMaster();
    }
    
    protected Broker createBroker()
    {
        ZooClient client = new ZooClient( storeDir, messageLog, storeIdGetter, ConfigProxy.config( config, ZooClient.Configuration.class ), responseReceiver );
        return new ZooKeeperBroker( ConfigProxy.config( config, ZooKeeperBroker.Configuration.class ), client);
    }

    // TODO This should be removed. Analyze usages
    public String getStoreDir()
    {
        return storeDir;
    }

    public enum BranchedDataPolicy
    {
        keep_all
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                moveAwayDb( db, branchedDataDir( db ) );
            }
        },
        keep_last
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                File branchedDataDir = branchedDataDir( db );
                moveAwayDb( db, branchedDataDir );
                for ( File file : new File( db.getStoreDir() ).listFiles() )
                {
                    if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                    {
                        try
                        {
                            FileUtils.deleteRecursively( file );
                        }
                        catch ( IOException e )
                        {
                            db.messageLog.logMessage( "Couldn't delete old branched data directory " + file, e );
                        }
                    }
                }
            }
        },
        keep_none
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                for ( File file : relevantDbFiles( db ) )
                {
                    try
                    {
                        FileUtils.deleteRecursively( file );
                    }
                    catch ( IOException e )
                    {
                        db.messageLog.logMessage( "Couldn't delete file " + file, e );
                    }
                }
            }
        },
        shutdown
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                db.shutdown();
            }
        };

        static String BRANCH_PREFIX = "branched-";

        abstract void handle( HighlyAvailableGraphDatabase db );

        protected void moveAwayDb( HighlyAvailableGraphDatabase db, File branchedDataDir )
        {
            for ( File file : relevantDbFiles( db ) )
            {
                File dest = new File( branchedDataDir, file.getName() );
                if ( !file.renameTo( dest ) ) db.messageLog.logMessage( "Couldn't move " + file.getPath() );
            }
        }

        File branchedDataDir( HighlyAvailableGraphDatabase db )
        {
            File result = new File( db.getStoreDir(), BRANCH_PREFIX + System.currentTimeMillis() );
            result.mkdirs();
            return result;
        }

        File[] relevantDbFiles( HighlyAvailableGraphDatabase db )
        {
            return new File( db.getStoreDir() ).listFiles( new FileFilter()
            {
                @Override
                public boolean accept( File file )
                {
                    return !file.getName().equals( StringLogger.DEFAULT_NAME ) && !isBranchedDataDirectory( file );
                }
            } );
        }

        boolean isBranchedDataDirectory( File file )
        {
            return file.isDirectory() && file.getName().startsWith( BRANCH_PREFIX );
        }
    }

    class HAResponseReceiver
        implements ResponseReceiver
    {
        @Override
        public SlaveContext getSlaveContext( int eventIdentifier )
        {
            // Constructs a slave context from scratch.
            XaDataSourceManager localDataSourceManager = localGraph().getXaDataSourceManager();
            Collection<XaDataSource> dataSources = localDataSourceManager.getAllRegisteredDataSources();
            @SuppressWarnings("unchecked")
            Pair<String, Long>[] txs = new Pair[dataSources.size()];
            int i = 0;
            for ( XaDataSource dataSource : dataSources )
            {
                txs[i++] = Pair.of( dataSource.getName(), dataSource.getLastCommittedTxId() );
            }
            return new SlaveContext( startupTime, machineId, eventIdentifier, txs );
        }

        @Override
        public <T> T receive( Response<T> response )
        {
            try
            {
                MasterUtil.applyReceivedTransactions( response, HighlyAvailableGraphDatabase.this, MasterUtil.NO_ACTION );
                updateTime();
                return response.response();
            }
            catch ( IOException e )
            {
                newMaster( e );
                throw new RuntimeException( e );
            }
        }

        @Override
        public void handle( Exception e )
        {
            newMaster( e );
        }

        @Override
        public void newMaster( Exception e )
        {
            HighlyAvailableGraphDatabase.this.newMaster( null, e );
        }

        /**
         * Shuts down the broker, invalidating every connection to the zookeeper
         * cluster and starts it again. Should be called in case a ConnectionExpired
         * event is received, this is the equivalent of building the ZK connection
         * from start. Also triggers a master reelect, to make sure that the state
         * ZK ended up in during our absence is respected.
         */
        @Override
        public void reconnect( Exception e )
        {
            if ( broker != null )
            {
                broker.shutdown();
            }
            broker = createBroker();
            newMaster( e );
        }
        
        @Override
        public int getMasterForTx( long tx )
        {
            try
            {
                return localGraph().getXaDataSourceManager().getNeoStoreDataSource().getMasterForCommittedTx( tx ).first();
            }
            catch ( IOException e )
            {
                throw new ComException( "Master id not found for tx:" + tx, e );
            }
        }
    }

    private class TxManagerCheckKernelEventHandler
        implements KernelEventHandler
    {
        @Override
        public void beforeShutdown()
        {
        }

        @Override
        public void kernelPanic( ErrorState error )
        {
            if( error == ErrorState.TX_MANAGER_NOT_OK )
            {
                messageLog.logMessage( "TxManager not ok, doing internal restart" );
                internalShutdown( true );
                newMaster( null, new Exception( "Tx manager not ok" ) );
            }
        }

        @Override
        public Object getResource()
        {
            return null;
        }

        @Override
        public ExecutionOrder orderComparedTo( KernelEventHandler other )
        {
            return ExecutionOrder.DOESNT_MATTER;
        }
    }

    private class HANodeLookup
        implements NodeProxy.NodeLookup
    {
        @Override
        public NodeImpl lookup( long nodeId )
        {
            return localGraph().getNodeManager().getNodeForProxy( nodeId );
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return HighlyAvailableGraphDatabase.this;
        }

        @Override
        public NodeManager getNodeManager()
        {
            return localGraph().getNodeManager();
        }
    }

    private class HARelationshipLookups
        implements RelationshipProxy.RelationshipLookups
    {
        @Override
        public Node lookupNode( long nodeId )
        {
            return localGraph().getNodeManager().getNodeById( nodeId );
        }

        @Override
        public RelationshipImpl lookupRelationship( long relationshipId )
        {
            return localGraph().getNodeManager().getRelationshipForProxy( relationshipId );
        }

        @Override
        public GraphDatabaseService getGraphDatabaseService()
        {
            return HighlyAvailableGraphDatabase.this;
        }

        @Override
        public NodeManager getNodeManager()
        {
            return localGraph().getNodeManager();
        }
    }

    @Override
    public PersistenceSource getPersistenceSource()
    {
        return localGraph().getPersistenceSource();
    }
}
