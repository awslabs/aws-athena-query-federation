/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.neptune;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class NeptuneConnection
{
    private static Cluster cluster = null;
    private static Long queryExecutionTimeout = null;
    NeptuneConnection(String neptuneEndpoint, String neptunePort, boolean enabledIAM)
    {
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(neptuneEndpoint)
               .port(Integer.parseInt(neptunePort))
               .enableSsl(true);

        if (enabledIAM) {
            builder = builder.channelizer(SigV4WebSocketChannelizer.class);
        }
        queryExecutionTimeout = (System.getenv("query_execution_timeout") == null) ? null
                : Long.parseLong(System.getenv("query_execution_timeout"));
        cluster = builder.create();
    }

    public Client getNeptuneClientConnection()
    {
        return cluster.connect();
    }

    public GraphTraversalSource getTraversalSource(Client client)
    {
        DriverRemoteConnection connection = DriverRemoteConnection.using(client);
        GraphTraversalSource source = AnonymousTraversalSource.traversal().withRemote(connection);
        if (queryExecutionTimeout != null) {
          return source.with(Tokens.ARGS_EVAL_TIMEOUT, queryExecutionTimeout);
        }
        else {
          return source;
        }
    }

    void closeCluster()
    {
        cluster.close();
    }
}
