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
package com.amazonaws.athena.connectors.neptune.propertygraph;

import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NeptuneGremlinConnection extends NeptuneConnection 
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneGremlinConnection.class);
    private static Cluster cluster = null;

    public NeptuneGremlinConnection(String neptuneEndpoint, String neptunePort, boolean enabledIAM, String region)
    {
        super(neptuneEndpoint, neptunePort, enabledIAM, region);
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint(neptuneEndpoint)
               .port(Integer.parseInt(neptunePort))
               .enableSsl(true);
               
        if (enabledIAM) {
            logger.info("Connecting with IAM auth to https://" + neptuneEndpoint + ":" + neptunePort + " in " + region);
            final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
            builder.handshakeInterceptor(r ->
                    {
                        try {
                            NeptuneNettyHttpSigV4Signer sigV4Signer =
                                    new NeptuneNettyHttpSigV4Signer(region, awsCredentialsProvider);
                            sigV4Signer.signRequest(r);
                        }
                        catch (NeptuneSigV4SignerException e) {
                            logger.error("SIGV4 exception", e);
                            throw new RuntimeException("Exception occurred while signing the request", e);
                        }
                        return r;
                    }
            );
        }
        
        cluster = builder.create();
    }

    public Client getNeptuneClientConnection()
    {
        return cluster.connect();
    }

    public GraphTraversalSource getTraversalSource(Client client)
    {
        DriverRemoteConnection connection = DriverRemoteConnection.using(client);
        return AnonymousTraversalSource.traversal().withRemote(connection);
    }

    void closeCluster()
    {
        cluster.close();
    }
}
