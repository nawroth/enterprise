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
package remote;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.HighlyAvailableGraphDatabase.CONFIG_KEY_COORDINATORS;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.RemoteGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestRemoteGraphDatabase
{
    private LocalhostZooKeeperCluster zoo;
    private HighlyAvailableGraphDatabase master;
    
    @Before
    public void startZoo()
    {
        zoo = LocalhostZooKeeperCluster.standardZoo( TestRemoteGraphDatabase.class );
        master = new HighlyAvailableGraphDatabase( TargetDirectory.forTest( TestRemoteGraphDatabase.class ).directory( "remote", true ).getAbsolutePath(), stringMap( 
                CONFIG_KEY_COORDINATORS, zoo.getConnectionString(),
                HighlyAvailableGraphDatabase.CONFIG_KEY_SERVER_ID, "1" ) );
    }
    
    @After
    public void shutDownZoo()
    {
        master.shutdown();
        zoo.shutdown();
    }
    
    @Test
    public void createANode() throws Exception
    {
        GraphDatabaseService db = new RemoteGraphDatabase( stringMap( CONFIG_KEY_COORDINATORS, zoo.getConnectionString() ) );
        Transaction tx = db.beginTx();
        Node node = null;
        try
        {
            node = db.createNode();
            System.out.println( "Created node " + node );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        System.out.println( master.getNodeById( node.getId() ) );
        db.shutdown();
    }
}
