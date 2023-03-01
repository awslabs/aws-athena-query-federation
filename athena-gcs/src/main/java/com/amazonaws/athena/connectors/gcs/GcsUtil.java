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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.UTC_ZONE_ID;
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

    public static Object coerce(FieldVector vector, Object value)
    {
        switch (vector.getMinorType()) {
            case TIMESTAMPNANO:
            case TIMENANO:
                if (value instanceof LocalDateTime) {
                    return DateTimeFormatterUtil.packDateTimeWithZone(
                            ((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                }
                else if (value instanceof Date) {
                    long ldtInLong = Instant.ofEpochMilli(((Date) value).getTime())
                            .atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                    return DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                }
                else {
                    return Duration.ofNanos((Long) value).toMillis();
                }
            case TIMEMICRO:
            case TIMESTAMPMICRO:
                if (value instanceof LocalDateTime) {
                    return DateTimeFormatterUtil.packDateTimeWithZone(
                            ((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                }
                else {
                    return TimeUnit.MICROSECONDS.toMillis((Long) value);
                }
            case TIMESTAMPMICROTZ:
                if (value instanceof ZonedDateTime) {
                    return DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime) value);
                }
                else if (value instanceof Date) {
                    long ldtInLong = Instant.ofEpochMilli(((Date) value).getTime())
                            .atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                    return DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                }
                else {
                    return TimeUnit.MICROSECONDS.toMillis((Long) value);
                }
            default:
                return value;
        }
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
