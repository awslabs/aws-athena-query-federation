/*-
 * #%L
 * Amazon Athena GCS Connector
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

import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.sun.jna.platform.unix.LibC;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_LOCATION_PREFIX;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION_VALUE;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.SSL_CERT_FILE_LOCATION;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.SSL_CERT_FILE_LOCATION_VALUE;

public class GcsUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsUtil.class);

    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private GcsUtil()
    {
    }

    /**
     * Write out the cacerts that we trust from the default java truststore.
     * Code adapted from: https://stackoverflow.com/a/63678794
     */
    public static void installCaCertificate() throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateEncodingException
    {
        FileWriter caBundleWriter = new FileWriter(SSL_CERT_FILE_LOCATION_VALUE);
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
                X509TrustManager x509TrustManager = (X509TrustManager) trustManager;
                for (X509Certificate x509Certificate : x509TrustManager.getAcceptedIssuers()) {
                    caBundleWriter.write(formatCrtFileContents(x509Certificate));
                    caBundleWriter.write(LINE_SEPARATOR);
                }
            }
        }
        finally {
            caBundleWriter.close();
        }
    }

    /**
     * Install/place Google cloud platform credentials from AWS secret manager to temp location
     * This is required for dataset api
     */
    public static void installGoogleCredentialsJsonFile(java.util.Map<String, String> configOptions) throws IOException
    {
        CachableSecretsManager secretsManager = new CachableSecretsManager(AWSSecretsManagerClientBuilder.defaultClient());
        String gcsCredentialsJsonString = secretsManager.getSecret(configOptions.get(GCS_SECRET_KEY_ENV_VAR));
        File destination = new File(GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION_VALUE);
        boolean destinationDirExists = new File(destination.getParent()).mkdirs();
        if (!destinationDirExists && destination.exists()) {
            return;
        }
        try (OutputStream out = new FileOutputStream(destination)) {
            out.write(gcsCredentialsJsonString.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }

    /**
     * Builds a GCS uri
     *
     * @param bucketName bucket name
     * @param path folder path
     * @return String representation uri
     */
    public static String createUri(String bucketName, String path)
    {
        return GCS_LOCATION_PREFIX + bucketName + "/" + path;
    }

    /**
     * Builds a GCS uri
     *
     * @param path bucket path
     * @return String representation uri
     */
    public static String createUri(String path)
    {
        return GCS_LOCATION_PREFIX + path;
    }

    /**
     * Get AWS Glue table object
     *
     * @param tableName table info
     * @param awsGlue AWS Glue client
     * @return Table object
     */
    public static Table getGlueTable(TableName tableName, AWSGlue awsGlue)
    {
        GetTableRequest getTableRequest = new GetTableRequest();
        getTableRequest.setDatabaseName(tableName.getSchemaName());
        getTableRequest.setName(tableName.getTableName());

        GetTableResult result = awsGlue.getTable(getTableRequest);
        return result.getTable();
    }

    // The value returned here is going to block.offerValue, which eventually invokes BlockUtils.setValue()
    // setValue() will take various java date objects to set on the block, so its preferrable to return those
    // kinds of objects instead of just a raw long.
    // This generally means that we only have to coerce raw longs into proper java date objects so that
    // BlockUtils will just do the right thing depending on the target schema.
    public static Object coerce(FieldVector vector, Object value)
    {
        // Since [...]/gcs/storage/StorageMetadata.java is only mapping these
        // types in the schema, we only have to worry about Time and Timestamp
        //  case TIMESTAMPNANO:
        //  case TIMESTAMPSEC:
        //  case TIMESTAMPMILLI:
        //  case TIMEMICRO:
        //  case TIMESTAMPMICRO:
        //  case TIMENANO:
        //      return Types.MinorType.DATEMILLI.getType();
        //  case TIMESTAMPMILLITZ:
        //  case TIMESTAMPMICROTZ: {
        //      return new ArrowType.Timestamp(
        //          org.apache.arrow.vector.types.TimeUnit.MILLISECOND,
        //          ((ArrowType.Timestamp) arrowType).getTimezone());
        //  }
        ArrowType arrowType = vector.getField().getType();
        switch (arrowType.getTypeID()) {
            case Time: {
                ArrowType.Time actualType = (ArrowType.Time) arrowType;
                if (value instanceof Long) {
                    return Instant.EPOCH.plus(
                        (Long) value,
                        DateTimeFormatterUtil.arrowTimeUnitToChronoUnit(actualType.getUnit())
                    ).atZone(java.time.ZoneId.of("UTC")).toLocalDateTime();
                }
                // If its anything other than Long, just let BlockUtils handle it directly.
                return value;
            }
            case Timestamp: {
                ArrowType.Timestamp actualType = (ArrowType.Timestamp) arrowType;
                if (value instanceof Long) {
                    // Convert this long and timezone into a ZonedDateTime
                    // Since BlockUtils.setValue accepts ZonedDateTime objects for TIMESTAMPMILLITZ
                    return Instant.EPOCH.plus(
                        (Long) value,
                        DateTimeFormatterUtil.arrowTimeUnitToChronoUnit(actualType.getUnit())
                    ).atZone(java.time.ZoneId.of(actualType.getTimezone()));
                }
                // If its anything other than Long, just let BlockUtils handle it directly.
                return value;
            }
        }
        return value;
    }

    // Code adapted from: https://stackoverflow.com/a/40774458
    private static String formatCrtFileContents(Certificate certificate) throws CertificateEncodingException
    {
        Base64.Encoder encoder = Base64.getMimeEncoder(64, LINE_SEPARATOR.getBytes());
        byte[] rawCrtText = certificate.getEncoded();
        String encodedCertText = new String(encoder.encode(rawCrtText));
        String prettifiedCert = BEGIN_CERT + LINE_SEPARATOR + encodedCertText + LINE_SEPARATOR + END_CERT;
        return prettifiedCert;
    }

    public static void setupNativeEnvironmentVariables()
    {
        LibC.INSTANCE.setenv(GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION, GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION_VALUE, 1);
        LibC.INSTANCE.setenv(SSL_CERT_FILE_LOCATION, SSL_CERT_FILE_LOCATION_VALUE, 1);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Set native environment variables: {}: {} ; {}: {}",
                GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION, LibC.INSTANCE.getenv(GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION),
                SSL_CERT_FILE_LOCATION, LibC.INSTANCE.getenv(SSL_CERT_FILE_LOCATION));
        }
    }
}
