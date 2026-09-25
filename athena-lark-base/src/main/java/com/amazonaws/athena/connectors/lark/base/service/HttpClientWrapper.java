/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

package com.amazonaws.athena.connectors.lark.base.service;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class HttpClientWrapper
{
    private final CloseableHttpClient httpClient;

    public HttpClientWrapper()
    {
        this(HttpClients.createDefault());
    }

    /**
     * Constructor for testing with dependency injection.
     *
     * @param httpClient the HTTP client to use
     */
    public HttpClientWrapper(CloseableHttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    public CloseableHttpResponse execute(HttpPost request) throws IOException
    {
        return httpClient.execute(request);
    }

    public CloseableHttpResponse execute(HttpGet request) throws IOException
    {
        return httpClient.execute(request);
    }
}
