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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
//import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.Client;

public class NeptuneConnection {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneConnection.class);
    
    private static Cluster cluster = Cluster.build()
        .addContactPoint(System.getenv("neptune_endpoint"))
        .port(Integer.parseInt(System.getenv("neptune_port")))
        .enableSsl(true)
        .create();

    NeptuneConnection() {   
    }

    Client getNeptunClientConnection() {
        return cluster.connect();
    }

    GraphTraversalSource getTraversalSource(Client client) {
        DriverRemoteConnection connection = DriverRemoteConnection.using(client);
        return AnonymousTraversalSource.traversal().withRemote(connection);
    }

    void closeCluster() {
        cluster.close();
    }
}
