/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.connection;

public final class EnvironmentConstants
{
    private EnvironmentConstants() {}

    public static final int CONNECT_TIMEOUT = 2000;

    // Lambda environment variable keys
    public static final String DEFAULT_GLUE_CONNECTION = "glue_connection";
    public static final String SECRET_NAME = "secret_name";
    public static final String SPILL_KMS_KEY_ID = "spill_kms_key_id";
    public static final String KMS_KEY_ID = "kms_key_id";
    public static final String DEFAULT = "default";
    public static final String DEFAULT_DOCDB = "default_docdb";
    public static final String DEFAULT_HBASE = "default_hbase";

    // glue connection property names
    public static final String HOST = "HOST";
    public static final String PORT = "PORT";
    public static final String JDBC_PARAMS = "JDBC_PARAMS";
    public static final String DATABASE = "DATABASE";
    public static final String SESSION_CONFS = "SESSION_CONFS";
    public static final String HIVE_CONFS = "HIVE_CONFS";
    public static final String HIVE_VARS = "HIVE_VARS";
    public static final String WAREHOUSE = "WAREHOUSE";
    public static final String SCHEMA = "SCHEMA";
    public static final String PROJECT_ID = "PROJECT_ID";
    public static final String CLUSTER_RES_ID = "CLUSTER_RESOURCE_ID";
    public static final String GRAPH_TYPE = "GRAPH_TYPE";
    public static final String HBASE_PORT = "HBASE_PORT";
    public static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";
    public static final String CUSTOM_AUTH_TYPE = "CUSTOM_AUTH_TYPE";
    public static final String GLUE_CERTIFICATES_S3_REFERENCE = "CERTIFICATE_S3_REFERENCE";
    public static final String ENFORCE_SSL = "ENFORCE_SSL";
}
