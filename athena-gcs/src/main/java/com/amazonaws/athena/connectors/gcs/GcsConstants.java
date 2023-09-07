/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;

public class GcsConstants
{
    /**
     * A deserialized JSON from a String List to be added as a property
     * of a Split. This Split will be passed to the {@link GcsRecordHandler#readWithConstraint(BlockSpiller, ReadRecordsRequest, QueryStatusChecker)} to
     * help know from which file it will read the records
     */
    static final String STORAGE_SPLIT_JSON = "storage_split_json";
    static final int MAX_SPLITS_PER_REQUEST = 1000;

    /**
     * An environment variable in the deployed Lambda which is name of the secret in AWS Secrets Manager.
     * contains credential keys/values in the form of JSON to access the GCS buckets/objects
     */
    public static final String GCS_SECRET_KEY_ENV_VAR = "secret_manager_gcp_creds_name";

    /**
     * A ssl file location constant to store the SSL certificate
     * The file location is fixed at /tmp directory
     * to retrieve ssl certificate location
     */
    public static final String SSL_CERT_FILE_LOCATION = "SSL_CERT_FILE";
    public static final String SSL_CERT_FILE_LOCATION_VALUE = "/tmp/cacert.pem";

    /**
     * A file name constant to store the GCP service account's credential JSON
     * The file location is fixed at /tmp directory and the file used to access gs://.. like URI to read files
     * to retrieve metadata and fetch data
     */
    public static final String GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION = "GOOGLE_APPLICATION_CREDENTIALS";
    public static final String GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION_VALUE = "/tmp/service-account.json";

    /**
     * Glue Table classification to specify type of the data a Glue Table represents.
     * For example, PARQUET, CSV, etc.
     */
    public static final String CLASSIFICATION_GLUE_TABLE_PARAM = "classification";

    /**
     * Partition pattern parameter added as an additional parameter in a Glue Table to identify partition
     * folder pattern after the Table's location URI. A pattern consists of storage prefix with one or more  partition key variable placeholders
     * For example, for partition fields state and city of type String, partition folder can be like the following:
     * <ul>
     *     <li>state=Texas/city=Houston</li>
     *     <li>state=California/city=Los Angeles</li>
     *     <li>....</li>
     * </ul>
     * In such case the <code>partition.pattern</code> should look like the following:
     * <p>
     *     <code>state=${state}/city=${city}</code><br/>
     *     Where {state} and {city} are the partition key variable placeholders values of which will be determined at runtime
     * </p>
     *
     */
    public static final String PARTITION_PATTERN_KEY = "partition.pattern";

    /**
     * GCS location prefix
     */
    public static final String GCS_LOCATION_PREFIX = "gs://";

    /**
     * GCS file format
     * For example, PARQUET, CSV, etc.
     */
    public static final String FILE_FORMAT = "gcs_file_format";

    /**
     * default private constructor to prevent code-coverage util to consider a constructor for covering
     */
    private GcsConstants()
    {
    }
}
