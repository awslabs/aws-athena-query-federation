/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

/**
 * An {@link HttpRequestInterceptor} that signs requests using any AWS {@link AwsV4HttpSigner}
 * and {@link AwsCredentialsProvider}.
 */
public class AWSRequestSigningApacheInterceptor implements HttpRequestInterceptor
{
    /**
     * The service that we're connecting to. Technically not necessary.
     * Could be used by a future Signer, though.
     */
    private final String service;

    /**
     * The particular signer implementation.
     */
    private final AwsV4HttpSigner signer;

    /**
     * The source of AWS credentials for signing.
     */
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final String region;

    /**
     * @param service                service that we're connecting to
     * @param signer                 particular signer implementation
     * @param awsCredentialsProvider source of AWS credentials for signing
     */
    public AWSRequestSigningApacheInterceptor(final String service,
                                              final AwsV4HttpSigner signer,
                                              final AwsCredentialsProvider awsCredentialsProvider,
                                              final String region)
    {
        this.service = service;
        this.signer = signer;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.region = region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException
    {
        URIBuilder uriBuilder;
        try {
            uriBuilder = new URIBuilder(request.getRequestLine().getUri());
        }
        catch (URISyntaxException e) {
            throw new AthenaConnectorException("Invalid URI", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        // Build the SdkHttpFullRequest
        SdkHttpFullRequest.Builder signableRequest = null;
        try {
            signableRequest = SdkHttpFullRequest.builder()
                    .method(SdkHttpMethod.fromValue(request.getRequestLine().getMethod())) // Set HTTP Method
                    .encodedPath(uriBuilder.build().getRawPath())                          // Set Resource Path
                    .rawQueryParameters(nvpToMapParams(uriBuilder.getQueryParams()))    // Set Query Parameters
                    .headers(headerArrayToMap(request.getAllHeaders()));
        }
        catch (URISyntaxException e) {
            throw new AthenaConnectorException("Invalid URI", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        // Set the endpoint (host) if present in the context
        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host != null) {
            signableRequest.uri(URI.create(host.toURI()));  // Set the base endpoint URL
        }

        // Handle content/body if it's an HttpEntityEnclosingRequest
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            if (httpEntityEnclosingRequest.getEntity() != null) {
                InputStream contentStream = httpEntityEnclosingRequest.getEntity().getContent();
                signableRequest.contentStreamProvider(() -> contentStream);  // Set content provider
            }
            else {
                // Workaround: provide an empty stream if no entity is present
                signableRequest.contentStreamProvider(() -> new ByteArrayInputStream(new byte[0]));
            }
        }

        // Sign the request
        SdkHttpFullRequest.Builder finalSignableRequest = signableRequest;
        SignedRequest signedRequest =
                signer.sign(r -> r.identity(awsCredentialsProvider.resolveCredentials())
                        .request(finalSignableRequest.build())
                        .payload(finalSignableRequest.contentStreamProvider())
                        .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, service)
                        .putProperty(AwsV4HttpSigner.REGION_NAME, region)); // Required for S3 only
        // Now copy everything back to the original request (including signed headers)
        request.setHeaders(mapToHeaderArray(signedRequest.request().headers()));

        // If the request has an entity (body), copy it back to the original request
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            if (httpEntityEnclosingRequest.getEntity() != null) {
                BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
                basicHttpEntity.setContent(signableRequest.contentStreamProvider().newStream());
                httpEntityEnclosingRequest.setEntity(basicHttpEntity);
            }
        }
    }

    /**
     * @param params list of HTTP query params as NameValuePairs
     * @return a multimap of HTTP query params
     */
    private static Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params)
    {
        Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (NameValuePair nvp : params) {
            List<String> argsList =
                    parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
            argsList.add(nvp.getValue());
        }
        return parameterMap;
    }

    /**
     * @param headers modeled Header objects
     * @return a Map of header entries
     */
    private static Map<String, List<String>> headerArrayToMap(final Header[] headers)
    {
        Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Header header : headers) {
            if (!skipHeader(header)) {
                // If the header name already exists, add the new value to the list
                headersMap.computeIfAbsent(header.getName(), k -> new ArrayList<>()).add(header.getValue());
            }
        }
        return headersMap;
    }

    /**
     * @param header header line to check
     * @return true if the given header should be excluded when signing
     */
    private static boolean skipHeader(final Header header)
    {
        return ("content-length".equalsIgnoreCase(header.getName())
                && "0".equals(header.getValue())) // Strip Content-Length: 0
                || "host".equalsIgnoreCase(header.getName()); // Host comes from endpoint
    }

    /**
     * @param mapHeaders Map of header entries
     * @return modeled Header objects
     */
    private static Header[] mapToHeaderArray(final Map<String, List<String>> mapHeaders)
    {
        Header[] headers = new Header[mapHeaders.size()];
        int i = 0;
        for (Map.Entry<String, List<String>> headerEntry : mapHeaders.entrySet()) {
            headers[i++] = new BasicHeader(headerEntry.getKey(), headerEntry.getValue().get(0));
        }
        return headers;
    }
}
