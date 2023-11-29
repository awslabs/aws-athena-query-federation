/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.io.IOException;

/**
 * SPARQL repository for connecting to Neptune instances.
 *
 * The repository supports both unauthenticated connections as well as IAM using
 * Signature V4 auth (https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html).
 * There are two constructors, one for unauthenticated and one for authenticated connections.
 *
 * @author schmdtm
 */
public class NeptuneSparqlRepository extends SPARQLRepository 
{
    /**
     * URL of the Neptune endpoint (*without* the trailing "/sparql" servlet).
     */
    private final String endpointUrl;

    /**
     * The name of the region in which Neptune is running.
     */
    private final String regionName;

    /**
     * Whether or not authentication is enabled.
     */
    private final boolean authenticationEnabled;

    /**
     * The credentials provider, offering credentials for signing the request.
     */
    private final AWSCredentialsProvider awsCredentialsProvider;

    /**
     * The signature V4 signer used to sign the request.
     */
    private NeptuneSigV4Signer<HttpUriRequest> v4Signer;

    /**
     * Set up a NeptuneSparqlRepository with V4 signing disabled.
     *
     * @param endpointUrl the prefix of the Neptune endpoint (without "/sparql" suffix)
     */
    public NeptuneSparqlRepository(final String endpointUrl) 
    {
        super(getSparqlEndpoint(endpointUrl));

        // all the fields below are only relevant for authentication and can be ignored
        this.authenticationEnabled = false;
        this.endpointUrl = null; // only needed if auth is enabled
        this.awsCredentialsProvider = null; // only needed if auth is enabled
        this.regionName = null; // only needed if auth is enabled
    }

    /**
     * Set up a NeptuneSparqlRepository with V4 signing enabled.
     *
     * @param awsCredentialsProvider the credentials provider used for authentication
     * @param endpointUrl the prefix of the Neptune endpoint (without "/sparql" suffix)
     * @param regionName name of the region in which Neptune is running
     *
     * @throws NeptuneSigV4SignerException in case something goes wrong with signer initialization
     */
    public NeptuneSparqlRepository(
            final String endpointUrl, final AWSCredentialsProvider awsCredentialsProvider,
            final String regionName)
            throws NeptuneSigV4SignerException 
    {
        super(getSparqlEndpoint(endpointUrl));

        this.authenticationEnabled = true;
        this.endpointUrl = endpointUrl;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.regionName = regionName;

        initAuthenticatingHttpClient();
    }

    /**
     * Wrap the HTTP client to do Signature V4 signing using Apache HTTP's interceptor mechanism.
     *
     * @throws NeptuneSigV4SignerException in case something goes wrong with signer initialization
     */
    protected void initAuthenticatingHttpClient() throws NeptuneSigV4SignerException 
    {
        if (!authenticationEnabled) {
            return; // auth not initialized, no signing performed
        }

        // init an V4 signer for Apache HTTP requests
        v4Signer = new NeptuneApacheHttpSigV4Signer(regionName, awsCredentialsProvider);

        /*
         *  Set an interceptor that signs the request before sending it to the server
         * => note that we add our interceptor last to make sure we operate on the final
         *    version of the request as generated by the interceptor chain
         */
        final HttpClient v4SigningClient = HttpClientBuilder.create().addInterceptorLast(new HttpRequestInterceptor() 
        {
            @Override
            public void process(final HttpRequest req, final HttpContext ctx) throws HttpException, IOException 
            {
                if (req instanceof HttpUriRequest) {
                    final HttpUriRequest httpUriReq = (HttpUriRequest) req;
                    try {
                        v4Signer.signRequest(httpUriReq);
                    } 
                    catch (NeptuneSigV4SignerException e) {
                        throw new HttpException("Problem signing the request: ", e);
                    }
                } 
                else {
                    throw new HttpException("Not an HttpUriRequest"); // this should never happen
                }
            }
        }).build();

        setHttpClient(v4SigningClient);
    }

    /**
     * Append the "/sparql" servlet to the endpoint URL. This is fixed, by convention in Neptune.
     *
     * @param endpointUrl generic endpoint/server URL
     * @return the SPARQL endpoint URL for the given server
     */
    private static String getSparqlEndpoint(final String endpointUrl) 
    {
        return endpointUrl + "/sparql";
    }
}
