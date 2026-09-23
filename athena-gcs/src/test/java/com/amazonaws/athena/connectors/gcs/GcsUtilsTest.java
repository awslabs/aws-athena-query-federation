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

import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION_VALUE;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.SSL_CERT_FILE_LOCATION_VALUE;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.coerce;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GcsUtilsTest
{
    private RootAllocator allocator = null;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void tearDown() {
        allocator.close();
    }

    protected RootAllocator rootAllocator() {
        return allocator;
    }

    @Test
    public void createUri_withBucketAndPath_returnsGsUri()
    {
        String uri = GcsUtil.createUri("bucket", "test");
        assertEquals("gs://bucket/test", uri);
    }

    @Test
    public void createUri_withFullPath_returnsGsUri()
    {
        String uri = GcsUtil.createUri("bucket/test");
        assertEquals("gs://bucket/test", uri);
    }

    @Test
    public void installCaCertificate_whenCalled_writesCertificateToTempFile() throws Exception
    {
        final String algorithm = "PKIX";
        File certFile = new File(SSL_CERT_FILE_LOCATION_VALUE);
        if (certFile.getParentFile() != null) {
            certFile.getParentFile().mkdirs();
        }

        X509Certificate mockCertificate = mock(X509Certificate.class);
        X509TrustManager mockTrustManager = mock(X509TrustManager.class);
        TrustManagerFactory mockTrustManagerFactory = mock(TrustManagerFactory.class);
        when(mockCertificate.getEncoded()).thenReturn("test-certificate-data".getBytes(StandardCharsets.UTF_8));
        when(mockTrustManager.getAcceptedIssuers()).thenReturn(new X509Certificate[]{mockCertificate});
        when(mockTrustManagerFactory.getTrustManagers()).thenReturn(new TrustManager[]{mockTrustManager});

        try (MockedStatic<TrustManagerFactory> trustManagerFactoryStatic = mockStatic(TrustManagerFactory.class)) {
            trustManagerFactoryStatic.when(TrustManagerFactory::getDefaultAlgorithm).thenReturn(algorithm);
            trustManagerFactoryStatic.when(() -> TrustManagerFactory.getInstance(algorithm))
                    .thenReturn(mockTrustManagerFactory);

            GcsUtil.installCaCertificate();

            trustManagerFactoryStatic.verify(TrustManagerFactory::getDefaultAlgorithm);
            trustManagerFactoryStatic.verify(() -> TrustManagerFactory.getInstance(algorithm));
            verify(mockTrustManagerFactory).init((KeyStore) isNull());
            verify(mockTrustManager).getAcceptedIssuers();
            verify(mockCertificate).getEncoded();

            assertTrue(certFile.exists());
            String written = new String(Files.readAllBytes(certFile.toPath()), StandardCharsets.UTF_8);
            assertTrue(written.contains("-----BEGIN CERTIFICATE-----"));
            assertTrue(written.contains("-----END CERTIFICATE-----"));
        }
        finally {
            if (certFile.exists()) {
                certFile.delete();
            }
        }
    }

    @Test
    public void installGoogleCredentialsJsonFile_withExistingOrMissingFile_writesOrSkipsAsExpected()
    {
        // Test when file doesn't exist - should create file and write content
        testInstallGoogleCredentialsJsonFileScenario(false, true, 1);

        // Test when file already exists - should return early without writing
        testInstallGoogleCredentialsJsonFileScenario(true, false, 0);
    }

    private void testInstallGoogleCredentialsJsonFileScenario(boolean fileExists, boolean mkdirsResult, int expectedFileOutputStreamConstructions)
    {
        final String secretKey = "test-secret-key";
        final String expectedCredentialsContent = "{\"type\": \"service_account\", \"project_id\": \"test-project\"}";
        final String existingFileContent = "{\"existing\": true}";
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(GCS_SECRET_KEY_ENV_VAR, secretKey);

        File destination = new File(GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION_VALUE);
        if (destination.getParentFile() != null) {
            destination.getParentFile().mkdirs();
        }

        try {
            if (fileExists) {
                Files.write(destination.toPath(), existingFileContent.getBytes(StandardCharsets.UTF_8));
            }
            else if (destination.exists()) {
                destination.delete();
            }

            try (MockedStatic<SecretsManagerClient> secretsManagerClientStatic = mockStatic(SecretsManagerClient.class);
                 MockedConstruction<CachableSecretsManager> secretsManagerConstruction = mockConstruction(
                         CachableSecretsManager.class,
                         (mock, context) -> when(mock.getSecret(secretKey)).thenReturn(expectedCredentialsContent))) {
                secretsManagerClientStatic.when(SecretsManagerClient::create)
                        .thenReturn(mock(SecretsManagerClient.class));

                GcsUtil.installGoogleCredentialsJsonFile(configOptions);

                secretsManagerClientStatic.verify(SecretsManagerClient::create);
                assertEquals(1, secretsManagerConstruction.constructed().size());
                verify(secretsManagerConstruction.constructed().get(0)).getSecret(secretKey);

                assertTrue(destination.exists());
                String actual = new String(Files.readAllBytes(destination.toPath()), StandardCharsets.UTF_8);
                if (expectedFileOutputStreamConstructions > 0) {
                    assertEquals(expectedCredentialsContent, actual);
                }
                else {
                    assertEquals(existingFileContent, actual);
                }
            }
        }
        catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
        finally {
            if (destination.exists()) {
                destination.delete();
            }
        }
    }

    @Test
    public void coerce_withTimestampAndTimeVectors_returnsExpectedDateTimeValues()
    {
        Instant instant = Instant.now();
        long seconds = instant.getEpochSecond();
        long micros = java.util.concurrent.TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.MICRO_OF_SECOND);
        long nanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.NANO_OF_SECOND);
        long millis = java.util.concurrent.TimeUnit.SECONDS.toMillis(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.MILLI_OF_SECOND);

        ZonedDateTime zonedDateTime = instant.atZone(java.time.ZoneId.of("UTC"));
        LocalDateTime localDateTime = instant.atZone(java.time.ZoneId.of("UTC")).toLocalDateTime();

        // Test all of the types that are being coerced in StorageMetadata
        TimeStampNanoVector timestampNanoVector = new TimeStampNanoVector(Field.nullable("timestamp_nano_col", Types.MinorType.TIMESTAMPNANO.getType()), allocator);
        timestampNanoVector.allocateNew();
        timestampNanoVector.setSafe(0, nanos);
        timestampNanoVector.setValueCount(1);

        TimeStampSecVector timestampSecVector = new TimeStampSecVector(Field.nullable("timestamp_sec_col", Types.MinorType.TIMESTAMPSEC.getType()), allocator);
        timestampSecVector.allocateNew();
        timestampSecVector.setSafe(0, seconds);
        timestampSecVector.setValueCount(1);

        TimeStampMilliVector timestampMilliVector = new TimeStampMilliVector(Field.nullable("timestamp_milli_col", Types.MinorType.TIMESTAMPMILLI.getType()), allocator);
        timestampMilliVector.allocateNew();
        timestampMilliVector.setSafe(0, millis);
        timestampMilliVector.setValueCount(1);

        TimeMicroVector timeMicroVector = new TimeMicroVector(Field.nullable("timemicro_col", Types.MinorType.TIMEMICRO.getType()), allocator);
        timeMicroVector.allocateNew();
        timeMicroVector.setSafe(0, micros);
        timeMicroVector.setValueCount(1);

        TimeStampMicroVector timestampMicroVector = new TimeStampMicroVector(Field.nullable("timestamp_micro_col", Types.MinorType.TIMESTAMPMICRO.getType()), allocator);
        timestampMicroVector.allocateNew();
        timestampMicroVector.setSafe(0, micros);
        timestampMicroVector.setValueCount(1);

        TimeNanoVector timeNanoVector = new TimeNanoVector(Field.nullable("timenano_col", Types.MinorType.TIMENANO.getType()), allocator);
        timeNanoVector.allocateNew();
        timeNanoVector.setSafe(0, nanos);
        timeNanoVector.setValueCount(1);

        TimeStampMilliTZVector timeStampMilliTZVector = new TimeStampMilliTZVector(
            Field.nullable(
                "timestamp_millitz_col",
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
            allocator);
        timeStampMilliTZVector.allocateNew();
        timeStampMilliTZVector.setSafe(0, millis);
        timeStampMilliTZVector.setValueCount(1);

        TimeStampMicroTZVector timeStampMicroTZVector = new TimeStampMicroTZVector(
            Field.nullable(
                "timestamp_microtz_col",
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")),
            allocator);
        timeStampMicroTZVector.allocateNew();
        timeStampMicroTZVector.setSafe(0, micros);
        timeStampMicroTZVector.setValueCount(1);

        try {
            // test coercing
            // This will hit the Timestamp non long case (LocalDateTime)
            assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), coerce(timestampNanoVector, timestampNanoVector.getObject(0)));
            assertEquals(localDateTime.truncatedTo(ChronoUnit.SECONDS), coerce(timestampSecVector, timestampSecVector.getObject(0)));
            assertEquals(localDateTime.truncatedTo(ChronoUnit.MILLIS), coerce(timestampMilliVector, timestampMilliVector.getObject(0)));

            // This will hit the Time long case
            assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), coerce(timeMicroVector, timeMicroVector.getObject(0)));

            // This will hit the Timestamp non long case (LocalDateTime)
            assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), coerce(timestampMicroVector, timestampMicroVector.getObject(0)));

            // This will hit the Time long case
            assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), coerce(timeNanoVector, timeNanoVector.getObject(0)));

            // These cases will hit the Timestamp long case
            assertEquals(zonedDateTime.truncatedTo(ChronoUnit.MILLIS), coerce(timeStampMilliTZVector, timeStampMilliTZVector.getObject(0)));
            assertEquals(zonedDateTime.truncatedTo(ChronoUnit.MICROS), coerce(timeStampMicroTZVector, timeStampMicroTZVector.getObject(0)));
        }
        finally {
            // Close all the vectors
            VectorSchemaRoot.of(
                timestampNanoVector,
                timestampSecVector,
                timestampMilliVector,
                timeMicroVector,
                timestampMicroVector,
                timeNanoVector,
                timeStampMilliTZVector,
                timeStampMicroTZVector).close();
        }
    }
}
