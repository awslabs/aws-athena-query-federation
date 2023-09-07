/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;

import static com.amazonaws.athena.connectors.gcs.GcsUtil.installCaCertificate;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.installGoogleCredentialsJsonFile;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.setupNativeEnvironmentVariables;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data.
 */
@SuppressWarnings("unused")
public class GcsCompositeHandler
        extends CompositeHandler
{
    private static final BufferAllocator allocator = new RootAllocator();
    /**
     * The default constructor that initializes metadata and record handlers for GCS
     */
    public GcsCompositeHandler() throws IOException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException
    {
        super(new GcsMetadataHandler(allocator, System.getenv()), new GcsRecordHandler(allocator, System.getenv()));
        installCaCertificate();
        installGoogleCredentialsJsonFile(System.getenv());
        setupNativeEnvironmentVariables();
    }
}
