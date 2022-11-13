/*-
 * #%L
 * athena-storage-api
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
package com.amazonaws.athena.storage;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.gcs.cache.CustomGcsReadChannel;
import com.amazonaws.athena.storage.gcs.io.*;
import com.amazonaws.athena.storage.mock.GcsMarker;
import com.amazonaws.athena.storage.mock.StorageMock;
import com.amazonaws.services.s3.AmazonS3;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.PageImpl;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Rule;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({AbstractStorageDatasource.class, FileCacheFactory.class, StorageFile.class, StorageDatasourceFactory.class, Job.class, ByteBuffer.class})
public class GcsTestBase extends StorageMock
{
    // bucket and file names
    public static final String BUCKET = "test";
    public static final String PARQUET_FILE = "customer-info.parquet";
    public static final String PARQUET_FILE_4_DATATYPE_TEST = "data_type_test.parquet";
    public static final String PARQUET_FILE_4_STREAM = "customer-info-4-stream-test.parquet";
    public static final String PARQUET_FILE_4_EMPTY_STREAM = "customer-info-4-empty-stream-test.parquet";
    public static final String PARQUET_TABLE = "customer_info";
    public static final String PARQUET_TABLE_4 = "customer-info-4-stream-test";
    public static final String CSV_FILE = "dimeemployee.csv";
    public static final String CSV_TABLE = "dimeemployee";
    protected static final Map<String, String> properties = Map.of(
            "max_partitions_size", "100"
            , "records_per_split", "1000"
            , "storage_split_json", "{ \"uid\": \"eeac1870-327f-4985-96fb-b5f3bcb75545\", \"fileName\": \"customer-info-4-stream-test.parquet\", \"groupSplits\": [ { \"groupIndex\": 0, \"rowOffset\": 500001, \"rowCount\": 500000, \"startRowIndex\": 500001, \"endRowIndex\": 1000000, \"hasNext\": false } ] }"
            , "objectName", "customer-info-4-stream-test"
    );
    public static final String gcsCredentialsJson = "";
    @Rule
    public PowerMockRule rule = new PowerMockRule();

    // mocking stuffs
    @Mock
    protected PageImpl<Bucket> blob;

    @Mock
    protected PageImpl<Blob> tables;

    public Storage mockStorageWithBlobIterator(String bucketName, Long blobSize, String fileName) throws Exception
    {
        Storage storage = mockStorageWithBlobIterator(bucketName);
        if (blobSize != null && blobSize > 0) {
            Blob blobObject = mock(Blob.class);
            doReturn(blobSize).when(blobObject).getSize();
            doReturn(blobObject).when(storage).get(ArgumentMatchers.any(BlobId.class));
        }
        Job readJob = mock(Job.class);
        mockStatic(Job.class);
        when(Job.getInstance()).thenReturn(readJob);
        Blob table = mock(Blob.class);
        when(table.getName()).thenReturn(fileName);
        when(table.getSize()).thenReturn(11L);
        when(tables.getValues()).thenReturn(List.of(table));
        doReturn(tables).when(storage).list(anyString());
        //doReturn(tables).when(storage).list(anyString());
        doReturn(List.of(table)).when(tables).iterateAll();
        return storage;
    }

    public Storage mockStorageWithBlobIterator(String bucketName)
    {
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        PowerMockito.when(storage.list()).thenReturn(blob);
        PowerMockito.when(storage.list(anyString(), Mockito.any())).thenReturn(tables);
        doReturn(blob).when(storage).list(anyString());
        PowerMockito.when(blob.iterateAll()).thenReturn(List.of(bucket));
        PowerMockito.when(bucket.getName()).thenReturn(bucketName);

        return storage;
    }

    public StorageWithInputTest mockStorageWithInputFile(String bucketName, String fileName) throws Exception
    {
        URL parquetFileResourceUri = ClassLoader.getSystemResource(fileName);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        RandomAccessFile file = new RandomAccessFile(parquetFile, "r");
        Storage storage = mockStorageWithBlobIterator(bucketName, file.length(), fileName);
        StorageFile storageFile = new StorageFile()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName);
        mockStatic(FileCacheFactory.class);
        GcsInputFile inputFile = new GcsInputFile(storageFile);
        PowerMockito.when(FileCacheFactory.getGCSInputFile(storage, bucketName, fileName)).thenReturn(inputFile);
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(ArgumentMatchers.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped(ArgumentMatchers.<List<String>>any())).thenReturn(credentials);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        PowerMockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        PowerMockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        PowerMockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        PowerMockito.when(mockedOptions.getService()).thenReturn(storage);

        return new StorageWithInputTest(storage, inputFile, storageFile);
    }

    public StorageWithStreamTest mockStorageWithInputStream(String bucketName, String fileName) throws Exception
    {
        URL fileResourceUri = ClassLoader.getSystemResource(fileName);
        File csvFile = new File(fileResourceUri.toURI());
        Storage storage = mockStorageWithBlobIterator(bucketName, csvFile.length(), fileName);

        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName);
        GcsOfflineStream gcsOfflineStream = new GcsOfflineStream()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName)
                .file(csvFile)
                .inputStream(new FileInputStream(csvFile));
        mockStatic(FileCacheFactory.class);
        PowerMockito.when(FileCacheFactory.createOfflineGcsStream(storage, bucketName, fileName)).thenReturn(gcsOfflineStream);
        PowerMockito.when(FileCacheFactory.createOnlineGcsStream(storage, bucketName, fileName)).thenReturn(gcsOnlineStream);
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(ArgumentMatchers.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped(ArgumentMatchers.<List<String>>any())).thenReturn(credentials);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        PowerMockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        PowerMockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        PowerMockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        PowerMockito.when(mockedOptions.getService()).thenReturn(storage);
        GcsStorageProvider storageProvider = mock(GcsStorageProvider.class);
        PowerMockito.whenNew(GcsStorageProvider.class).withAnyArguments().thenReturn(storageProvider);
        return new StorageWithStreamTest(storage, gcsOnlineStream);
    }

    public StorageWithStreamTest mockStorageWithInputStreamLargeFiles(String bucketName, String fileName) throws Exception
    {
        URL fileResourceUri = ClassLoader.getSystemResource(fileName);
        File csvFile = new File(fileResourceUri.toURI());
        Storage storage = mockStorageWithBlobIterator(bucketName, 157286401L, fileName);
        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName);
        GcsOfflineStream gcsOfflineStream = new GcsOfflineStream()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName)
                .file(csvFile)
                .inputStream(new FileInputStream(csvFile));
        mockStatic(FileCacheFactory.class);
        PowerMockito.when(FileCacheFactory.createOfflineGcsStream(storage, bucketName, fileName)).thenReturn(gcsOfflineStream);
        PowerMockito.when(FileCacheFactory.createOnlineGcsStream(storage, bucketName, fileName)).thenReturn(gcsOnlineStream);
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(ArgumentMatchers.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped(ArgumentMatchers.<List<String>>any())).thenReturn(credentials);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        PowerMockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        PowerMockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        PowerMockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        PowerMockito.when(mockedOptions.getService()).thenReturn(storage);
        GcsStorageProvider storageProvider = mock(GcsStorageProvider.class);
        PowerMockito.whenNew(GcsStorageProvider.class).withAnyArguments().thenReturn(storageProvider);
        return new StorageWithStreamTest(storage, gcsOnlineStream);
    }

    public StorageWithInputTest mockStorageWithEmptyInputFile(String bucketName, String fileName) throws Exception
    {
        URL parquetFileResourceUri = ClassLoader.getSystemResource(fileName);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        RandomAccessFile file = new RandomAccessFile(parquetFile, "r");
        Storage storage = mockStorageWithBlobIterator(bucketName, file.length(), fileName);
        StorageFile storageFile = new StorageFile()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName);
        mockStatic(FileCacheFactory.class);
        GcsInputFile inputFile = new GcsInputFile(storageFile);
        PowerMockito.when(FileCacheFactory.getGCSInputFile(storage, bucketName, fileName)).thenReturn(inputFile);
        PowerMockito.when(FileCacheFactory.getEmptyGCSInputFile(storage, bucketName, fileName)).thenReturn(inputFile);
        return new StorageWithInputTest(storage, inputFile, storageFile);
    }

    protected StorageDatasource getTestDataSource(final String extension) throws Exception
    {
        String appCredentialsJsonString = "{\n" +
                "  \"type\": \"service_account\",\n" +
                "  \"project_id\": \"athena-federated-query\",\n" +
                "  \"private_key_id\": \"2ad4078f1692a6d3887ab6c4a97254c755566646\",\n" +
                "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDIPRq3S8SHygD/\\nDXjq927DtcJ2cLSn0Gl3hFuArAUJjtRb8q0/Zw17drzkH2X+OrtMCutI8rwRmk/M\\nbR3nwetaAS/lSTtw+oN4PeYHAFokjLa12VqJl+zIM2GE0DLXRdar0YlAvwJ89P0B\\nV1/Uk7wdeOGxEfLrc5TIhxV5adV+QFS7jjgRBnkFxsMRMqKb1OMDvaHreqaiXJpP\\nd75Ut1prwB2JLkElfqMBBVcm8o1pSqsEGUamluQVEDK3E96XolsGPVe31k0o7rzf\\nyLP7MUvcwniIq4lrzSjeZ25N3FPkquUGbn+mPsNkLxCbOwcrY/jDxXNOUZTMLeqp\\n7Vkt01OxAgMBAAECggEACoNy0KbwxbRsgvfBHo0pSqtTb4aRZbJCp1zStRnTFeJi\\n8gT25bpSceYVGuEvKL8KsH5uRiFAkKgKgpBEHrQG2G3xhtdmgJMWrgyJ9FonYX3l\\n5fxojYrlislv2FpaIQVwtQrGaxjcV5VBZ32f8XhkSyParcJkf8pMyI6XKQ3OgNdm\\nCTVDD4C60KNq9a4sJqKBopB+FJLn4TnUGLZiZAV+A4uKLUA38zwga6oMWCygxgZz\\nE5eWy17I211NqSju9PErsGH6z4zApR4pTWKctpfn+dZQTBS8ZSoC7g2BBxLk3C0w\\niYlUQXt/KUCZ0xaI+6mQqq1QrvGCa54EKArjc+oxNQKBgQDw9Lst+U5QDNFD4vEg\\n9/NwsOxc79pREG2FgLrw4lZqfHX8o8llg7b8Y1c2ysrYj0B/0apCQVIVDZoRHerj\\nJr0dyXWklotRiXPZ67itPXhun95GXVdUNiSRYwwJEwkIhK584d90j5eXtTCyr5FM\\nWuZadBsAAtv4ZgqIYObiLy3yOwKBgQDUvZO1Jhf1r4MHnhCQ1Nv+2qgnhEtp3uHQ\\ni5h9N3cH0DmPBOisp1qcSlIYwUM8ufVBcEhNTsIAffbDAViWa05ju9/wUcIA6jBi\\nR8P8PF/Ex8TdZtgUCTabysfcONKOLZ2Sa7E0/O4Y9EZgnzsFkZWfx/eSvtW0a3Za\\neB19tH+nAwKBgQCPtIXF/3/zQhG0eS7ySK7JsNrm+q2r1y5ahtH3RCXh0GTVziEZ\\nCBskH2MubHfZ/GWtVbBDX43CvJ/8QWmLG9mCYFpnVNm2QVH00B8OQzEGWRZJxPWG\\nZdwdUYMmDlI+4FLobBXHALSaaBepGgiAD15+5+wKb6odVU5G0/QfRaATbQKBgQDJ\\n+suYO3iYDHDs9Idp2o6cYuEv04z+EVx38XFvwQ9D3dAoF1MJSULgDDfxxNufdjaC\\nUKZ0r4fFi9KSxl5jQbIFQsSUmCsHT1FsnhJXEsMiQ0CHrDMOosi0FUy3q0NNNcXa\\n1GBEnLc5/gIrjkItQVG7h9FoA8NGLpkJv+zQAmUIHQKBgQCG4I9+AsyDJsJvhAyy\\nT5T4oFIHHMD4ODZl+2rdmtEW07KbHatPme0hcMTwlVyZdN/AFJpZZ94Rm0/Zc6c8\\nwaz0nY3XnDw39+6TI6STjLeANIdc1GGTOVsUydsdr7P/MvRHrMABzbbPC7vp7N0v\\nf7lV2ouT0TltRUnyQ3GsqR31fw==\\n-----END PRIVATE KEY-----\\n\",\n" +
                "  \"client_email\": \"akshay-kachore-trianz-com@athena-federated-query.iam.gserviceaccount.com\",\n" +
                "  \"client_id\": \"105657958960441983486\",\n" +
                "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
                "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
                "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
                "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/akshay-kachore-trianz-com%40athena-federated-query.iam.gserviceaccount.com\"\n" +
                "}";
        return StorageDatasourceFactory.createDatasource(appCredentialsJsonString, Map.of(FILE_EXTENSION_ENV_VAR, extension));
    }

    protected Map<String, ValueSet> createSummaryWithLValueRangeEqual(String fieldName, ArrowType fieldType, Object fieldValue)
    {
        // Bypassing the lookup of the OS username
        // Please see here: https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));

        Block block = Mockito.mock(Block.class);
        FieldReader fieldReader = Mockito.mock(FieldReader.class);
        Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(fieldName, fieldType));

        Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
        Marker low = new GcsMarker(block, Marker.Bound.EXACTLY, false).withValue(fieldValue);
        return Map.of(
                fieldName, SortedRangeSet.of(false, new Range(low, low))
        );
    }

    protected Map<String, ValueSet> createMultiFieldSummaryWithLValueRangeEqual(List<String> fieldNames,
                                                                                List<ArrowType> fieldTypes,
                                                                                List<Object> fieldValues)
    {
        // Bypassing the lookup of the OS username
        // Please see here: https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));

        Map<String, ValueSet> valueSetMap = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            ArrowType fieldType = fieldTypes.get(i);
            Object fieldValue = fieldValues.get(i);
            Block block = Mockito.mock(Block.class);
            FieldReader fieldReader = Mockito.mock(FieldReader.class);
            Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(fieldName, fieldType));

            Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
            Marker marker = new GcsMarker(block, Marker.Bound.EXACTLY, false).withValue(fieldValue);
            valueSetMap.put(fieldName, SortedRangeSet.of(false, toRange(marker)));
        }
        return ImmutableMap.<String, ValueSet>builder().putAll(valueSetMap).build();
    }

    protected Map<String, ValueSet> createSummaryWithInClause(String fieldName, ArrowType fieldType, List<Object> fieldValues)
    {
        // Bypassing the lookup of the OS username
        // Please see here: https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));
        List<Marker> markers = new ArrayList<>();
        for (Object fieldValue : fieldValues) {
            Block block = Mockito.mock(Block.class);
            FieldReader fieldReader = Mockito.mock(FieldReader.class);
            Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(fieldName, fieldType));

            Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
            markers.add(new GcsMarker(block, Marker.Bound.EXACTLY, false).withValue(fieldValue));
        }
        return Map.of(fieldName, SortedRangeSet.of(false, toRange(markers.get(0)),
                toRanges(markers.subList(1, markers.size()))));
    }

    protected Range toRange(Marker marker)
    {
        return new Range(marker, marker);
    }

    protected List<Range> toRanges(List<Marker> markers)
    {
        List<Range> ranges = new ArrayList<>();
        for (Marker marker : markers) {
            ranges.add(new Range(marker, marker));
        }
        return ranges;
    }

    protected FileCacheFactoryInfoTest prepareFileCacheFactoryForStorage(String fileName) throws IOException, URISyntaxException
    {
        if (fileName == null) {
            fileName = PARQUET_FILE;
        }
        String tempFileName = String.format("%s_%s", BUCKET, fileName);
        final File tmpFile = File.createTempFile(tempFileName, "cache");
        tmpFile.deleteOnExit();
        URL parquetFileResourceUri = ClassLoader.getSystemResource(fileName);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        try (OutputStream out = new FileOutputStream(tmpFile)) {
            Files.copy(parquetFile.toPath(), out);
        }

        Storage storage = mock(Storage.class);
        Blob blobObject = mock(Blob.class);
        doReturn(parquetFile.length()).when(blobObject).getSize();
        ReadChannel readChannel = mock(ReadChannel.class);
        ReadChannel channel = new CustomGcsReadChannel(tmpFile);
        doReturn(channel).when(storage).reader(any(BlobId.class));
        mockStatic(ByteBuffer.class);
        ByteBuffer byteBuffer = mock(ByteBuffer.class);
        PowerMockito.when(ByteBuffer.allocate(anyInt())).thenReturn(byteBuffer);
        PowerMockito.when(ByteBuffer.allocateDirect(anyInt())).thenReturn(byteBuffer);
        when(byteBuffer.alignedSlice(anyInt())).thenReturn(byteBuffer);
        doReturn(new byte[(int) tmpFile.length()]).when(byteBuffer).array();
        doReturn(channel.read(byteBuffer)).when(readChannel).read(ArgumentMatchers.any(ByteBuffer.class));
        doReturn(blobObject).when(storage).get(any(BlobId.class));
        return new FileCacheFactoryInfoTest(storage, tmpFile);
    }

    protected FileCacheFactoryInfoTest prepareFileCacheFactory(String fileName, String cachePrefix) throws IOException, URISyntaxException
    {
        if (fileName == null) {
            fileName = PARQUET_FILE;
        }
        String tempFileName = String.format("%s_%s", BUCKET, fileName);
        final File tmpFile = File.createTempFile(tempFileName, cachePrefix);
        tmpFile.deleteOnExit();
        URL parquetFileResourceUri = ClassLoader.getSystemResource(fileName);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        try (OutputStream out = new FileOutputStream(tmpFile)) {
            Files.copy(parquetFile.toPath(), out);
        }

        Storage storage = mock(Storage.class);
        Blob blobObject = mock(Blob.class);
        doReturn(parquetFile.length()).when(blobObject).getSize();
        ReadChannel readChannel = mock(ReadChannel.class);
        ReadChannel channel = new CustomGcsReadChannel(tmpFile);
        doReturn(readChannel).when(storage).reader(any(BlobId.class));
        mockStatic(ByteBuffer.class);
        ByteBuffer byteBuffer = mock(ByteBuffer.class);
        PowerMockito.when(ByteBuffer.allocate(anyInt())).thenReturn(byteBuffer);
        PowerMockito.when(ByteBuffer.allocateDirect(anyInt())).thenReturn(byteBuffer);
        when(byteBuffer.alignedSlice(anyInt())).thenReturn(byteBuffer);
        doReturn(new byte[(int) tmpFile.length()]).when(byteBuffer).array();
        doReturn(channel.read(byteBuffer)).when(readChannel).read(ArgumentMatchers.any(ByteBuffer.class));
        doReturn(blobObject).when(storage).get(any(BlobId.class));
        return new FileCacheFactoryInfoTest(storage, tmpFile);
    }

    public S3BlockSpiller getS3SpillerObject(Schema schemaForRead)
    {
        AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        EncryptionKeyFactory keyFactory = new LocalKeyFactory();
        EncryptionKey encryptionKey = keyFactory.create();
        String queryId = UUID.randomUUID().toString();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(queryId)
                .withIsDirectory(true)
                .build();
        BlockAllocatorImpl allocator = new BlockAllocatorImpl();
        //Create Spill config
        SpillConfig spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                //This will be enough for a single block
                .withMaxBlockBytes(100000)
                //This will force the writer to spill.
                .withMaxInlineBlockBytes(100)
                //Async Writing.
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();
        return new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator());
    }

    protected static class StorageWithInputTest
    {
        private final Storage storage;
        private final GcsInputFile inputFile;
        private final StorageFile storageFile;

        public StorageWithInputTest(Storage storage, GcsInputFile inputFile, StorageFile storageFile)
        {
            requireNonNull(storage, "Storage was null");
            requireNonNull(inputFile, "GCS input file was null");
            this.storage = storage;
            this.inputFile = inputFile;
            this.storageFile = storageFile;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public GcsInputFile getInputFile()
        {
            return inputFile;
        }

        public StorageFile getFileCache()
        {
            return storageFile;
        }
    }

    protected static class StorageWithStreamTest
    {
        private final Storage storage;
        private final GcsOnlineStream gcsOnlineStream;

        public StorageWithStreamTest(Storage storage, GcsOnlineStream gcsOnlineStream)
        {
            requireNonNull(storage, "Storage was null");
            requireNonNull(gcsOnlineStream, "GCS input file was null");
            this.storage = storage;
            this.gcsOnlineStream = gcsOnlineStream;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public GcsOnlineStream getStreamCache()
        {
            return gcsOnlineStream;
        }
    }

    protected static class FileCacheFactoryInfoTest
    {
        private final Storage storage;
        private final File tmpFile;

        public FileCacheFactoryInfoTest(Storage storage, File tmpFile)
        {
            this.storage = storage;
            this.tmpFile = tmpFile;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public File getTmpFile()
        {
            return tmpFile;
        }
    }


}
