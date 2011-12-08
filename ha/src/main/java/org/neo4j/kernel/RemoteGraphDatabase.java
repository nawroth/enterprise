/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Collection;
import java.util.Map;

import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxHook;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.CommandExecutor;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class RemoteGraphDatabase extends AbstractGraphDatabase implements ResponseReceiver
{
    private static final CommandExecutor NOOP_COMMAND_EXECUTOR = new CommandExecutor()
    {
        @Override
        public void execute( XaCommand command )
        {
        }
    };
    
    private final Broker broker;
    private final EmbeddedGraphDbImpl localDb;
    private final long sessionId = System.currentTimeMillis();
    private final int machineId;

    public RemoteGraphDatabase( Map<String, String> config )
    {
        super( "nothing" );
        this.machineId = generateMachineId();
        this.broker = new ZooKeeperBroker( this,
                HighlyAvailableGraphDatabase.CONFIG_DEFAULT_HA_CLUSTER_NAME, machineId,
                HighlyAvailableGraphDatabase.getCoordinatorsFromConfig( config ),
                "noone:1234", 0,
                HighlyAvailableGraphDatabase.getClientReadTimeoutFromConfig( config ),
                HighlyAvailableGraphDatabase.getMaxConcurrentChannelsPerSlaveFromConfig( config ),
                false, this );
        this.localDb = new EmbeddedGraphDbImpl( getStoreDir(), new StoreId(), config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxHook( broker, this ),
                CommonFactories.defaultLastCommittedTxIdSetter(),
                new EphemeralFileSystemAbstraction(),
                NOOP_COMMAND_EXECUTOR );
    }
    
    private int generateMachineId()
    {
        return 10;
    }

    @Override
    protected StringLogger createStringLogger()
    {
        return StringLogger.DEV_NULL;
    }

    @Override
    public Node createNode()
    {
        return localDb.createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getReferenceNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction beginTx()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexManager index()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void close()
    {
        localDb.shutdown();
    }

    @Override
    public Config getConfig()
    {
        return localDb.getConfig();
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return localDb.getManagementBeans( type );
    }

    @Override
    public KernelData getKernelData()
    {
        return localDb.getKernelData();
    }

    @Override
    public SlaveContext getSlaveContext( int eventIdentifier )
    {
        return new SlaveContext( sessionId, machineId, eventIdentifier, new Pair[0] );
    }

    @Override
    public <T> T receive( Response<T> response )
    {
        return response.response();
    }

    @Override
    public void newMaster( Exception cause )
    {
        System.out.println( "newMaster called" );
    }
}
