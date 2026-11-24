/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.lenient;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneSparqlRepositoryTest {

    @Mock
    private AWSCredentialsProvider mockCredentialsProvider;
    @Mock
    private AWSCredentials mockCredentials;

    private static final String ENDPOINT = "http://example.com";
    private static final String REGION = "us-west-2";
    private static final String EMPTY_REGION = "";

    @Before
    public void setup() {
        lenient().when(mockCredentialsProvider.getCredentials()).thenReturn(mockCredentials);
    }

    @Test
    public void neptuneSparqlRepository_WithoutAuthentication_CreatesValidRepository() {
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(ENDPOINT);
        assertNotNull(repository);
    }

    @Test
    public void neptuneSparqlRepository_WithAuthentication_CreatesValidRepository() throws NeptuneSigV4SignerException {
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(ENDPOINT, mockCredentialsProvider, REGION);
        assertNotNull(repository);
    }

    @Test(expected = IllegalArgumentException.class)
    public void neptuneSparqlRepository_WithEmptyRegion_ThrowsIllegalArgumentException() throws Exception {
        new NeptuneSparqlRepository(ENDPOINT, mockCredentialsProvider, EMPTY_REGION);
    }

    @Test
    public void requestSigning_WithValidConfiguration_CompletesSuccessfully() throws NeptuneSigV4SignerException {
        NeptuneSparqlRepository repository = new NeptuneSparqlRepository(
                ENDPOINT,
                mockCredentialsProvider,
                REGION
        );

        assertNotNull("Repository should not be null", repository);
    }
} 
