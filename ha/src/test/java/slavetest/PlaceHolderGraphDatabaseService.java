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
package slavetest;

import java.util.Collection;
import javax.transaction.TransactionManager;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.GraphDatabaseSPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.TransactionBuilder;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;

public class PlaceHolderGraphDatabaseService implements GraphDatabaseSPI
{
    private volatile GraphDatabaseSPI db;

    public PlaceHolderGraphDatabaseService( )
    {
    }

    public void setDb( GraphDatabaseSPI db )
    {
        this.db = db;
    }

    public GraphDatabaseSPI getDb()
    {
        return db;
    }

    @Override
    public Node createNode()
    {
        return db.createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        return db.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return db.getRelationshipById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return db.getReferenceNode();
    }

    @Override
    public Transaction beginTx()
    {
        return db.beginTx();
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return db.registerTransactionEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return db.unregisterTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return db.registerKernelEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return db.unregisterKernelEventHandler( handler );
    }

    @Override
    public IndexManager index()
    {
        return db.index();
    }

    @Override
    public NodeManager getNodeManager()
    {
        return db.getNodeManager();
    }

    @Override
    public LockReleaser getLockReleaser()
    {
        return db.getLockReleaser();
    }

    @Override
    public LockManager getLockManager()
    {
        return db.getLockManager();
    }

    @Override
    public XaDataSourceManager getXaDataSourceManager()
    {
        return db.getXaDataSourceManager();
    }

    @Override
    public TransactionManager getTxManager()
    {
        return db.getTxManager();
    }

    @Override
    public DiagnosticsManager getDiagnosticsManager()
    {
        return db.getDiagnosticsManager();
    }

    @Override
    public StringLogger getMessageLog()
    {
        return db.getMessageLog();
    }

    @Override
    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return db.getRelationshipTypeHolder();
    }

    @Override
    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return db.getIdGeneratorFactory();
    }

    @Override
    public String getStoreDir()
    {
        return db.getStoreDir();
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return db.getAllNodes();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return db.getRelationshipTypes();
    }

    @Override
    public void shutdown()
    {
        db.shutdown();
    }

    @Override
    public KernelData getKernelData()
    {
        return db.getKernelData();
    }

    @Override
    public <T> T getSingleManagementBean( Class<T> type )
    {
        return db.getSingleManagementBean( type );
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return db.getManagementBeans( type );
    }

    @Override
    public TransactionBuilder tx()
    {
        return db.tx();
    }
}
