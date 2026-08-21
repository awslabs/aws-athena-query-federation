/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseKerberosUtilsTest
{
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_PREFIX = "test-prefix";
    private static final String TEST_S3_URI = "s3://" + TEST_BUCKET + "/" + TEST_PREFIX + "/";

    @After
    public void tearDown()
            throws Exception
    {
        // Clean up temp directory created by tests
        Path kerberosConfigDir = Paths.get("/tmp/hbasekerberosconfigs");
        if (Files.exists(kerberosConfigDir)) {
            Files.walk(kerberosConfigDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void constants_withAllConstants_returnsExpectedValues()
    {
        assertEquals("KERBEROS_CONFIG_FILES_S3_REFERENCE should match", "kerberos_config_files_s3_reference", HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE);
        assertEquals("TEMP_DIR should match", "/tmp", HbaseKerberosUtils.TEMP_DIR);
        assertEquals("PRINCIPAL_NAME should match", "principal_name", HbaseKerberosUtils.PRINCIPAL_NAME);
        assertEquals("HBASE_RPC_PROTECTION should match", "hbase_rpc_protection", HbaseKerberosUtils.HBASE_RPC_PROTECTION);
        assertEquals("KERBEROS_AUTH_ENABLED should match", "kerberos_auth_enabled", HbaseKerberosUtils.KERBEROS_AUTH_ENABLED);
        assertEquals("HBASE_SECURITY_AUTHENTICATION should match", "kerberos", HbaseKerberosUtils.HBASE_SECURITY_AUTHENTICATION);
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withMissingConfig_throwsIllegalArgumentException() {
        Map<String, String> configOptions = ImmutableMap.of();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions));
        assertTrue("Exception message should contain not been populated", ex.getMessage().contains("not been populated"));
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withEmptyConfig_throwsIllegalArgumentException() {
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, ""
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions));
        assertTrue("Exception message should contain not been populated", ex.getMessage().contains("not been populated"));
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withInvalidS3Uri_throwsException()
    {
        assertCopyConfigFilesThrowsException("invalid-uri");
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withEmptyS3Uri_throwsException()
    {
        assertCopyConfigFilesThrowsException("s3://");
    }

    private void assertCopyConfigFilesThrowsException(String s3Uri)
    {
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, s3Uri
        );

        Exception ex = assertThrows(Exception.class, () ->
                HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions));
        assertTrue("Exception message should not be empty",
                ex.getMessage() != null && !ex.getMessage().isEmpty());
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withS3AccessError_throwsException()
    {
        // This tests the path where S3 URI is valid but S3Client.listObjects() throws an exception
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI
        );

        try (MockedStatic<S3Client> s3ClientMock = mockStatic(S3Client.class)) {
            S3Client mockS3Client = mock(S3Client.class);
            s3ClientMock.when(S3Client::create).thenReturn(mockS3Client);
            when(mockS3Client.listObjects(any(ListObjectsRequest.class)))
                    .thenThrow(new RuntimeException("Access Denied"));

            Exception ex = assertThrows(Exception.class, () ->
                    HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions));
            assertTrue("Exception should contain access error",
                    ex.getMessage() != null && ex.getMessage().contains("Access Denied"));
        }
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withS3UriMissingBucket_throwsException()
    {
        // Tests the path where S3 URI format is invalid (missing bucket after s3://)
        assertCopyConfigFilesThrowsException("s3:///prefix");
    }


    @Test
    public void copyConfigFilesFromS3ToTempFolder_withS3Objects_copiesFilesToTempDirectory()
            throws Exception
    {
        // This test covers lines 80-93: the loop that processes S3 objects and copies them
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI
        );

        try (MockedStatic<S3Client> s3ClientMock = mockStatic(S3Client.class)) {
            List<S3Object> s3Objects = new ArrayList<>();
            s3Objects.add(S3Object.builder().key(TEST_PREFIX + "/krb5.conf").build());
            s3Objects.add(S3Object.builder().key(TEST_PREFIX + "/hbase.keytab").build());

            List<ResponseInputStream<GetObjectResponse>> responseStreams = new ArrayList<>();
            responseStreams.add(new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    new ByteArrayInputStream("krb5.conf content".getBytes())
            ));
            responseStreams.add(new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    new ByteArrayInputStream("keytab content".getBytes())
            ));

            setupS3ClientMock(s3ClientMock, s3Objects, responseStreams);

            Path result = HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions);

            assertNotNull("Temp directory path should not be null", result);
            assertTrue("Temp directory should exist", Files.exists(result));

            // Verify files were copied (lines 80-91)
            File krb5File = new File(result.toFile(), "krb5.conf");
            File keytabFile = new File(result.toFile(), "hbase.keytab");
            assertTrue("krb5.conf file should exist", krb5File.exists());
            assertTrue("hbase.keytab file should exist", keytabFile.exists());
        }
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withEmptyFileName_skipsFileCopy()
            throws Exception
    {
        // This test covers line 88: if (!fName.isEmpty()) check
        // When file name is empty after extraction, file copy is skipped
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI
        );

        try (MockedStatic<S3Client> s3ClientMock = mockStatic(S3Client.class)) {
            // Create S3 object with key that results in empty file name
            // Key "test-prefix/" will result in empty fName after substring
            List<S3Object> s3Objects = new ArrayList<>();
            s3Objects.add(S3Object.builder().key(TEST_PREFIX + "/").build());

            setupS3ClientMock(s3ClientMock, s3Objects, null);

            Path result = HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions);

            assertNotNull("Temp directory path should not be null", result);
            // Verify file copy was skipped: no files should exist in the temp directory
            File[] filesInDir = result.toFile().listFiles();
            assertTrue("File copy should be skipped for empty file name; directory should have no copied files",
                    filesInDir == null || filesInDir.length == 0);
        }
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_withMultipleFiles_processesAllFiles()
            throws Exception
    {
        // This test covers the loop (lines 80-92) with multiple files
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, "s3://" + TEST_BUCKET + "/config/"
        );

        try (MockedStatic<S3Client> s3ClientMock = mockStatic(S3Client.class)) {
            List<S3Object> s3Objects = new ArrayList<>();
            s3Objects.add(S3Object.builder().key("config/file1.txt").build());
            s3Objects.add(S3Object.builder().key("config/file2.txt").build());
            s3Objects.add(S3Object.builder().key("config/file3.txt").build());

            List<ResponseInputStream<GetObjectResponse>> responseStreams = new ArrayList<>();
            responseStreams.add(new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    new ByteArrayInputStream("content1".getBytes())
            ));
            responseStreams.add(new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    new ByteArrayInputStream("content2".getBytes())
            ));
            responseStreams.add(new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    new ByteArrayInputStream("content3".getBytes())
            ));

            setupS3ClientMock(s3ClientMock, s3Objects, responseStreams);

            Path result = HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions);

            assertNotNull("Temp directory path should not be null", result);
            assertTrue("Temp directory should exist", Files.exists(result));

            // Verify all files were processed
            File file1 = new File(result.toFile(), "file1.txt");
            File file2 = new File(result.toFile(), "file2.txt");
            File file3 = new File(result.toFile(), "file3.txt");
            assertTrue("file1.txt should exist", file1.exists());
            assertTrue("file2.txt should exist", file2.exists());
            assertTrue("file3.txt should exist", file3.exists());
        }
    }

    @Test
    public void copyConfigFilesFromS3ToTempFolder_whenTempDirectoryNotExists_createsTempDirectory()
            throws Exception
    {
        // This test covers lines 108-110: mkdirs() call when directory doesn't exist
        // The getTempDirPath() method is called internally, so we test it indirectly
        Map<String, String> configOptions = ImmutableMap.of(
                HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI
        );

        try (MockedStatic<S3Client> s3ClientMock = mockStatic(S3Client.class)) {
            // Return empty list so no files are copied, but temp dir is still created
            setupS3ClientMock(s3ClientMock, Collections.emptyList(), null);

            Path result = HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(configOptions);

            assertNotNull("Temp directory path should not be null", result);
            // The directory should be created (either it existed or mkdirs() was called)
            assertTrue("Temp directory should exist", Files.exists(result));
        }
    }

    private void setupS3ClientMock(MockedStatic<S3Client> s3ClientMock, List<S3Object> s3Objects, 
                                    List<ResponseInputStream<GetObjectResponse>> responseStreams)
    {
        S3Client mockS3Client = mock(S3Client.class);
        s3ClientMock.when(S3Client::create).thenReturn(mockS3Client);

        ListObjectsResponse listResponse = ListObjectsResponse.builder()
                .contents(s3Objects != null ? s3Objects : Collections.emptyList())
                .build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(listResponse);

        if (responseStreams != null && !responseStreams.isEmpty()) {
            when(mockS3Client.getObject(any(GetObjectRequest.class)))
                    .thenAnswer(invocation -> responseStreams.remove(0));
        }
    }
}

