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
package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;

import static com.amazonaws.athena.connectors.vertica.VerticaSchemaUtils.installCaCertificate;
import static com.amazonaws.athena.connectors.vertica.VerticaSchemaUtils.setupNativeEnvironmentVariables;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data.
 */
public class VerticaCompositeHandler
        extends CompositeHandler
{
    public VerticaCompositeHandler() throws CertificateEncodingException, IOException, NoSuchAlgorithmException, KeyStoreException
    {
        super(new VerticaMetadataHandler(System.getenv()), new VerticaRecordHandler(System.getenv()));
        installCaCertificate();
        setupNativeEnvironmentVariables();
    }
}
