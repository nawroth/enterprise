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

import org.neo4j.com.Client;
import org.neo4j.kernel.ha.TimeUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.neo4j.backup.OnlineBackupExtension.parsePort;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;

public class HaConfig
{
    public static final String CONFIG_KEY_OLD_SERVER_ID = "ha.machine_id";
    public static final String CONFIG_KEY_SERVER_ID = "ha.server_id";
    public static final String CONFIG_KEY_OLD_COORDINATORS = "ha.zoo_keeper_servers";
    public static final String CONFIG_KEY_COORDINATORS = "ha.coordinators";
    public static final String CONFIG_KEY_SERVER = "ha.server";
    public static final String CONFIG_KEY_CLUSTER_NAME = "ha.cluster_name";
    public static final String CONFIG_KEY_PULL_INTERVAL = "ha.pull_interval";
    public static final String CONFIG_KEY_READ_TIMEOUT = "ha.read_timeout";
    public static final String CONFIG_KEY_LOCK_READ_TIMEOUT = "ha.lock_read_timeout";
    public static final String CONFIG_KEY_COORDINATOR_FETCH_INFO_TIMEOUT = "ha.coordinator_fetch_info_timeout";

    public static final String CONFIG_DEFAULT_HA_CLUSTER_NAME = "neo4j.ha";
    public static final int CONFIG_DEFAULT_PORT = 6361;
    public static final long CONFIG_DEFAULT_PULL_INTERVAL = -1;
    public static final int CONFIG_DEFAULT_COORDINATOR_FETCH_INFO_TIMEOUT = 500;

    public static int getClientReadTimeoutFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_READ_TIMEOUT );
        return value != null ? Integer.parseInt( value ) : Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS;
    }

    public static int getClientLockReadTimeoutFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_LOCK_READ_TIMEOUT );
        return value != null ? Integer.parseInt( value ) : getClientReadTimeoutFromConfig( config );
    }

    public static long getPullIntervalFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_PULL_INTERVAL );
        return value != null ? TimeUtil.parseTimeMillis( value ) : HaConfig.CONFIG_DEFAULT_PULL_INTERVAL;
    }
}
