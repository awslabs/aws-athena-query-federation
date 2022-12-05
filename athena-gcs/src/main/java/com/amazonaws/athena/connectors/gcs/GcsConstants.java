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
     * A deserialized JSON from an instance of {@link com.amazonaws.athena.connectors.gcs.storage.StorageSplit} to be added as a property
     * of a Split. This Split will be passed to the {@link GcsRecordHandler#readWithConstraint(BlockSpiller, ReadRecordsRequest, QueryStatusChecker)} to
     * help know from which file it will read the records, along with record offset and total count of records to read
     */
    static final String STORAGE_SPLIT_JSON = "storage_split_json";
    static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    /**
     * An environment variable in the deployed Lambda that says the name of the secret in AWS Secrets Manager. This in ture,
     * contains credential keys/other values in the form of JSON to access the GCS buckets/objects
     */
    public static final String GCS_SECRET_KEY_ENV_VAR = "gcs_secret_name";

    /**
     * An environment variable in the deployed Lambda that says the key name under the configured secret that
     * contains credential keys/other values in the form of  JSON to access the GCS buckets/objects
     */
    public static final String GCS_CREDENTIAL_KEYS_ENV_VAR = "gcs_credential_key";

    /**
     * A ssl file location constant to store the SSL cerificate
     * The file location is fixed at /tmp directory
     * to retrieve ssl certificate location
     */
    public static final String SSL_CERT_FILE_LOCATION = "SSL_CERT_FILE";

    /**
     * A file name constant to store the GCP service account's credential JSON
     * The file location is fixed at /tmp directory and the file used to access gs://.. like URI to read files
     * to retrieve metadata and fetch data
     */
    public static final String GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION = "GOOGLE_APPLICATION_CREDENTIALS";

    /**
     * default private constructor to prevent code-coverage util to consider a constructor for covering
     */
    private GcsConstants()
    {
    }
}
