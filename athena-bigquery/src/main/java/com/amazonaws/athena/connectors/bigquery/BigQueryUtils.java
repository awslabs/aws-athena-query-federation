/*-
 * #%L
 * athena-bigquery
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

package com.amazonaws.athena.connectors.bigquery;

import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.ByteArrayInputStream;
import java.io.IOException;

class BigQueryUtils
{
    private BigQueryUtils() {}

    static Credentials getCredentialsFromSecretsManager()
        throws IOException
    {
        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
        GetSecretValueResult response = secretsManager.getSecretValue(new GetSecretValueRequest()
            .withSecretId(getEnvBigQueryCredsSmId()));
        return ServiceAccountCredentials.fromStream(new ByteArrayInputStream(response.getSecretString().getBytes()));
    }

    static BigQuery getBigQueryClient()
        throws IOException
    {
        BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder();
        bigqueryBuilder.setCredentials(getCredentialsFromSecretsManager());
        return bigqueryBuilder.build().getService();
    }

    static ResourceManager getResourceManagerClient()
        throws IOException
    {
        ResourceManagerOptions.Builder resourceManagerBuilder = ResourceManagerOptions.newBuilder();
        resourceManagerBuilder.setCredentials(getCredentialsFromSecretsManager());
        return resourceManagerBuilder.build().getService();
    }

    static String getEnvBigQueryCredsSmId()
    {
        return getEnvVar(BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID);
    }

    static String getEnvVar(String envVar)
    {
        String var = System.getenv(envVar);
        if (var == null || var.length() == 0) {
            throw new IllegalArgumentException("Lambda Environment Variable " + envVar + " has not been populated! ");
        }
        return var;
    }

    /**
     * Gets the project name that exists within Google Cloud Platform that contains the datasets that we wish to query.
     * The Lambda environment variables are first inspected and if it does not exist, then we take it from the catalog
     * name in the request.
     *
     * @param catalogNameFromRequest The Catalog Name from the request that is passed in from the Athena Connector framework.
     * @return The project name.
     */
    static String getProjectName(String catalogNameFromRequest)
    {
        if (System.getenv(BigQueryConstants.GCP_PROJECT_ID) != null) {
            return System.getenv(BigQueryConstants.GCP_PROJECT_ID);
        }
        return catalogNameFromRequest;
    }

    /**
     * Gets the project name that exists within Google Cloud Platform that contains the datasets that we wish to query.
     * The Lambda environment variables are first inspected and if it does not exist, then we take it from the catalog
     * name in the request.
     *
     * @param request The {@link MetadataRequest} from the request that is passed in from the Athena Connector framework.
     * @return The project name.
     */
    static String getProjectName(MetadataRequest request)
    {
        return getProjectName(request.getCatalogName());
    }

    /**
     * BigQuery is case sensitive for its Project and Dataset Names. This function will return the first
     * case insensitive match.
     *
     * @param projectName The dataset name we want to look up. The project name must be case correct.
     * @return A case correct dataset name.
     */
    static String fixCaseForDatasetName(String projectName, String datasetName, BigQuery bigQuery)
    {
        Page<Dataset> response = bigQuery.listDatasets(projectName);
        for (Dataset dataset : response.iterateAll()) {
            if (dataset.getDatasetId().getDataset().equalsIgnoreCase(datasetName)) {
                return dataset.getDatasetId().getDataset();
            }
        }

        throw new IllegalArgumentException("Google Dataset with name " + datasetName +
            " could not be found in Project " + projectName + " in GCP. ");
    }

    static String fixCaseForTableName(String projectName, String datasetName, String tableName, BigQuery bigQuery)
    {
        Page<Table> response = bigQuery.listTables(DatasetId.of(projectName, datasetName));
        for (Table table : response.iterateAll()) {
            if (table.getTableId().getTable().equalsIgnoreCase(tableName)) {
                return table.getTableId().getTable();
            }
        }
        throw new IllegalArgumentException("Google Table with name " + datasetName +
            " could not be found in Project " + projectName + " in GCP. ");
    }

    static Object getObjectFromFieldValue(String fieldName, FieldValue fieldValue, ArrowType arrowType)
    {
        if (fieldValue == null || fieldValue.isNull() || fieldValue.getValue().equals("null")) {
            return null;
        }
        switch (Types.getMinorTypeForArrowType(arrowType)) {
            case TIMESTAMPMILLI:
                //getTimestampValue() returns a long in microseconds. Return it in Milliseconds which is how its stored.
                return fieldValue.getTimestampValue() / 1000;
            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
                return fieldValue.getLongValue();
            case DECIMAL:
                return fieldValue.getNumericValue();
            case BIT:
                return fieldValue.getBooleanValue();
            case FLOAT4:
            case FLOAT8:
                return fieldValue.getDoubleValue();
            case VARCHAR:
                return fieldValue.getStringValue();
            //TODO: Support complex types.
            default:
                throw new IllegalArgumentException("Unknown type has been encountered: Field Name: " + fieldName +
                    " Field Type: " + arrowType.toString() + " MinorType: " + Types.getMinorTypeForArrowType(arrowType));
        }
    }

    static ArrowType translateToArrowType(LegacySQLTypeName type)
    {
        switch (type.getStandardType()) {
            case BOOL:
                return new ArrowType.Bool();
            /** A 64-bit signed integer value. */
            case INT64:
                return new ArrowType.Int(64, true);
            /** A 64-bit IEEE binary floating-point value. */
            case FLOAT64:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            /** A decimal value with 38 digits of precision and 9 digits of scale. */
            case NUMERIC:
                return new ArrowType.Decimal(38, 9);
            /** Variable-length character (Unicode) data. */
            case STRING:
                return new ArrowType.Utf8();
            /** Variable-length binary data. */
            case BYTES:
                return new ArrowType.Binary();
            /** Container of ordered fields each with a type (required) and field name (optional). */
            case STRUCT:
                return new ArrowType.Struct();
            /** Ordered list of zero or more elements of any non-array type. */
            case ARRAY:
                return new ArrowType.List();
            /**
             * Represents an absolute point in time, with microsecond precision. Values range between the
             * years 1 and 9999, inclusive.
             */
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            /** Represents a logical calendar date. Values range between the years 1 and 9999, inclusive. */
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            /** Represents a time, independent of a specific date, to microsecond precision. */
            case TIME:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            /** Represents a year, month, day, hour, minute, second, and subsecond (microsecond precision). */
            case DATETIME:
                return new ArrowType.Date(DateUnit.MILLISECOND);
            /** Represents a set of geographic points, represented as a Well Known Text (WKT) string. */
            case GEOGRAPHY:
                return new ArrowType.Utf8();
        }
        throw new IllegalArgumentException("Unable to map Google Type of StandardType: " + type.getStandardType().toString()
            + " NonStandardType: " + type.name());
    }
}
