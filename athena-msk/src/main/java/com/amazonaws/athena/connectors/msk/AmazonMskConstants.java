/*-
 * #%L
 * athena-msk
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.msk;

public class AmazonMskConstants
{
    public static final String KAFKA_SOURCE = "kafka";
    /**
     * schema is set to default
     */
    public static final String KAFKA_SCHEMA = "default";
    /**
     * This is the glue registry ARN that user need to provide while deploying lambda.
     * This glue registry will contain the mapping schemas
     */
    public static final String GLUE_REGISTRY_ARN = "glue_registry_arn";
    /**
     * For TLS authentication, user need to give S3 bucket reference where the client truststore and keystore
     * files are uploaded.
     */
    public static final String CERTIFICATES_S3_REFERENCE = "certificates_s3_reference";
    /**
     * This is the keystore password
     */
    public static final String KEYSTORE_PASSWORD = "keystore_password";
    /**
     * This is the truststore password
     */
    public static final String TRUSTSTORE_PASSWORD = "truststore_password";
    /**
     *This is SSL_KEY password
     */
    public static final String SSL_PASSWORD = "ssl_password";
    /**
     * This is kafka node details
     */
    public static final String ENV_KAFKA_ENDPOINT = "kafka_endpoint";
    /**
     * This is the type of authentication client has set for the cluster
     */
    public static final String AUTH_TYPE = "auth_type";
    /**
     * This is temp folder where the certificates from S3 will be downloaded
     */
    public static final String TEMP_DIR = "/tmp";
    /**
     * This is secret manager key reference
     */
    public static final String SECRET_MANAGER_MSK_CREDS_NAME = "secrets_manager_secret";
    /**
     * For scram authentication, we need to  fetch the credentials from secrets manager
     * and append to the property file
     */
    public static final String SCRAM_USERNAME = "username";
    public static final String SCRAM_PWD = "password";

    public static final int MAX_RECORDS_IN_SPLIT = 10_000;

    private AmazonMskConstants()
    {
    }
}
