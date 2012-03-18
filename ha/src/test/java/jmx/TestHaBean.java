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
package jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.ha.CreateEmptyDb;
import org.neo4j.ha.Neo4jHaCluster;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.jmx.Kernel;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.InstanceInfo;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestHaBean
{
    private static final TargetDirectory dir = TargetDirectory.forTest( TestHaBean.class );
    private static LocalhostZooKeeperCluster zk;
    private static HighlyAvailableGraphDatabase db;

    @BeforeClass
    public static void startDb() throws Exception
    {
        zk = LocalhostZooKeeperCluster.standardZoo( TestHaBean.class );
        File storeDir = dir.graphDbDir( /*clean=*/true );
        CreateEmptyDb.at( storeDir );
        db = Neo4jHaCluster.single( zk, storeDir, /*HA port:*/3377, //
                MapUtil.stringMap( "jmx.port", "9913" ) );
    }

    @AfterClass
    public static void stopDb()
    {
        if ( db != null ) db.shutdown();
        db = null;
        if ( zk != null ) zk.shutdown();
        zk = null;
        dir.cleanup();
    }

    @Test
    public void canGetHaBean() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( db.getManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        assertNotNull( "could not get ha bean", ha );
        assertTrue( "single instance should be master", ha.isMaster() );
    }

    @Test
    public void canGetBranchedStoreBean() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager(db.getSingleManagementBean( Kernel.class ) );
        BranchedStore bs = neo4j.getBranchedStoreBean();
        assertNotNull( "could not get ha bean", bs );
        assertEquals( "no branched stores for new db", 0,
                bs.getBranchedStores().length );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canGetInstanceConnectionInformation() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( db.getManagementBean( Kernel.class ) );
        InstanceInfo[] instances = neo4j.getHighAvailabilityBean().getInstancesInCluster();
        assertNotNull( instances );
        assertEquals( 1, instances.length );
        InstanceInfo instance = instances[0];
        assertNotNull( instance );
        String address = instance.getAddress();
        assertNotNull( "No JMX address for instance", address );
        String id = instance.getInstanceId();
        assertNotNull( "No instance id", id );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canConnectToInstance() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( db.getManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        InstanceInfo[] instances = ha.getInstancesInCluster();
        assertNotNull( instances );
        assertEquals( 1, instances.length );
        InstanceInfo instance = instances[0];
        assertNotNull( instance );
        Pair<Neo4jManager, HighAvailability> proc = instance.connect();
        assertNotNull( "could not connect", proc );
        neo4j = proc.first();
        ha = proc.other();
        assertNotNull( neo4j );
        assertNotNull( ha );

        instances = ha.getInstancesInCluster();
        assertNotNull( instances );
        assertEquals( 1, instances.length );
        assertEquals( instance.getAddress(), instances[0].getAddress() );
        assertEquals( instance.getInstanceId(), instances[0].getInstanceId() );
    }

    @Test
    public void dumpJmxInfo() throws Exception
    {
        StringBuilder beanInfo = new StringBuilder( 2048 );
        StringBuilder beanList = new StringBuilder( 2048 );
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Set<ObjectInstance> beans = mBeanServer
                .queryMBeans( new ObjectName( "org.neo4j:*" ),
                        null );
        for ( ObjectInstance bean : beans )
        {
            String name = bean.getObjectName()
                    .getKeyProperty( "name" );
            String name0 = bean.getObjectName()
                    .getKeyProperty( "name0" );
            if ( name0 != null )
            {
                name += " -- " + name0;
            }
            beanInfo.append( name );
            beanInfo.append( " (" );
            beanInfo.append( bean.getClassName() );
            beanInfo.append( ")\n" );
            ObjectName objectName = bean.getObjectName();
            beanList.append( objectName );
            beanList.append( '|' );
            MBeanInfo info = mBeanServer.getMBeanInfo( objectName );
            String description = info.getDescription()
                    .replace( '\n', ' ' );
            beanList.append( description );
            beanList.append( '\n' );
            beanInfo.append( description );
            beanInfo.append( '\n' );
            for ( MBeanAttributeInfo attrInfo : info.getAttributes() )
            {
                beanInfo.append( attrInfo.getName() );
                beanInfo.append( '|' );
                beanInfo.append( attrInfo.getDescription()
                        .replace( '\n', ' ' ) );
                beanInfo.append( '|' );
                beanInfo.append( attrInfo.getType() );
                beanInfo.append( '|' );
                beanInfo.append( attrInfo.isReadable() ? '✓' : '✕' );
                beanInfo.append( '|' );
                beanInfo.append( attrInfo.isWritable() ? '✓' : '✕' );
                beanInfo.append( '\n' );
            }
            beanInfo.append( '\n' );
            for ( MBeanOperationInfo operInfo : info.getOperations() )
            {
                beanInfo.append( operInfo.getName() );
                beanInfo.append( '|' );
                beanInfo.append( operInfo.getDescription()
                        .replace( '\n', ' ' ) );
                beanInfo.append( '|' );
                beanInfo.append( operInfo.getReturnType() );
                beanInfo.append( '|' );
                beanInfo.append( operInfo.getImpact() );
                beanInfo.append( '|' );
                MBeanParameterInfo[] params = operInfo.getSignature();
                for ( int i = 0; i < params.length; i++ )
                {
                    MBeanParameterInfo param = params[i];
                    beanInfo.append( param.getName() );
                    beanInfo.append( ':' );
                    beanInfo.append( param.getType() );
                    if ( i != ( params.length - 1 ) )
                    {
                        beanInfo.append( ',' );
                    }
                }
                beanInfo.append( '\n' );
            }
            beanInfo.append( '\n' );
        }
        Writer fw = null;
        try
        {
            fw = AsciiDocGenerator.getFW( "target/docs/ops", "JMX List" );
            fw.write( beanList.toString() );
            fw.write( beanInfo.toString() );
        }
        finally
        {
            if ( fw != null )
            {
                fw.close();
            }
        }
    }
}
