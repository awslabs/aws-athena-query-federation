
/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

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
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class BigQueryUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryUtils.class);
    private BigQueryUtils() {}

    public static Credentials getCredentialsFromSecretsManager()
            throws IOException
    {
        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest();
        getSecretValueRequest.setSecretId(getEnvBigQueryCredsSmId());
        GetSecretValueResult response = secretsManager.getSecretValue(getSecretValueRequest);
        return ServiceAccountCredentials.fromStream(new ByteArrayInputStream(response.getSecretString().getBytes()));
    }

    public static BigQuery getBigQueryClient() throws IOException
    {
        BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder();
        String endpoint = System.getenv(BigQueryConstants.BIG_QUERY_ENDPOINT);
        if (StringUtils.isNotEmpty(endpoint)) {
            bigqueryBuilder.setHost(endpoint);
        }
        bigqueryBuilder.setCredentials(getCredentialsFromSecretsManager());
        return bigqueryBuilder.build().getService();
    }

    public static String getEnvBigQueryCredsSmId()
    {
        return getEnvVar(BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID);
    }

    public static String getEnvVar(String envVar)
    {
        String envVariable = System.getenv(envVar);
        if (envVariable == null || envVariable.length() == 0) {
            throw new IllegalArgumentException("Lambda Environment Variable " + envVar + " has not been populated! ");
        }
        return envVariable;
    }

    /**
     * Gets the project name that exists within Google Cloud Platform that contains the datasets that we wish to query.
     * The Lambda environment variables are first inspected and if it does not exist, then we take it from the catalog
     * name in the request.
     *
     * @param catalogNameFromRequest The Catalog Name from the request that is passed in from the Athena Connector framework.
     * @return The project name.
     */
    public static String getProjectName(String catalogNameFromRequest)
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
    public static String fixCaseForDatasetName(String projectName, String datasetName, BigQuery bigQuery)
    {
        try {
            Page<Dataset> response = bigQuery.listDatasets(projectName);
            for (Dataset dataset : response.iterateAll()) {
                if (dataset.getDatasetId().getDataset().equalsIgnoreCase(datasetName)) {
                    return dataset.getDatasetId().getDataset();
                }
            }
            throw new IllegalArgumentException("Google Dataset with name " + datasetName +
                    " could not be found in Project " + projectName + " in GCP. ");
        }
        catch
        (Exception e) {
            LOGGER.error("Error: ", e);
        }
        return null;
    }

    public static String fixCaseForTableName(String projectName, String datasetName, String tableName, BigQuery bigQuery)
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

    static Object getObjectFromFieldValue(String fieldName, FieldValue fieldValue, ArrowType arrowType, boolean isTimeStampCol) throws ParseException
    {
        if (fieldValue.isNull() || fieldValue.getValue().equals("null")) {
            return null;
        }

        if ("Date(DAY)".equalsIgnoreCase(arrowType.toString())) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date d = sdf.parse((String) fieldValue.getValue());
                long milliSeconds = d.getTime();
                return java.util.concurrent.TimeUnit.MILLISECONDS.toDays(milliSeconds);
            }
            catch (ParseException e) {
                throw new ParseException(e.getMessage(), e.getErrorOffset());
            }
        }
        if ("Date(MILLISECOND)".equalsIgnoreCase(arrowType.toString())) {
            String dateTimeVal = (String) fieldValue.getValue();
            DateTimeFormatter dateTimeFormatter;
            LocalDateTime localDateTime;
            if (dateTimeVal.length() == 19) {
                try {
                    return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(dateTimeVal);
                }
                catch (ParseException e) {
                    throw new ParseException(e.getMessage(), e.getErrorOffset());
                }
            }
            else {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                localDateTime = LocalDateTime.parse(dateTimeVal, dateTimeFormatter);
                return localDateTime;
            }
        }
        switch (Types.getMinorTypeForArrowType(arrowType)) {
            case TIMESTAMPMILLI:
                // getTimestampValue() returns a long in microseconds. Return it in Milliseconds which is how its stored.
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
                if (isTimeStampCol) {
                    BigDecimal decimalSeconds = new BigDecimal(fieldValue.getStringValue().replace("T", " "));
                    long seconds = decimalSeconds.longValue();
                    long nanos = decimalSeconds.subtract(BigDecimal.valueOf(seconds))
                            .movePointRight(9)
                            .longValueExact();
                    return Instant.ofEpochSecond(seconds, nanos);
                }
                else {
                    return fieldValue.getStringValue();
                }
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
            /** Variable-length binary data. */
            case BYTES:
                return new ArrowType.Binary();
            /** Container of ordered fields each with a type (required) and field name (optional). */
            case STRUCT:
                return new ArrowType.Struct();
            /** Ordered list of zero or more elements of any non-array type. */
            case ARRAY:
                return new ArrowType.List();
            /** Represents a logical calendar date. Values range between the years 1 and 9999, inclusive. */
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            /** Represents a year, month, day, hour, minute, second, and subsecond (microsecond precision). */
            case DATETIME:
                return new ArrowType.Date(DateUnit.MILLISECOND);
            /** Represents a set of geographic points, represented as a Well Known Text (WKT) string. */
            default:
                return new ArrowType.Utf8();
        }
    }
}
