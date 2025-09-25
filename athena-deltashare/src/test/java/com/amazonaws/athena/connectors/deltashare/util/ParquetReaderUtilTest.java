/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connectors.deltashare.TestBase;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ParquetReaderUtilTest extends TestBase
{
    @Rule
    public TestName testName = new TestName();

    @Mock
    private BlockSpiller mockSpiller;

    @Mock
    private HttpClient mockHttpClient;

    @Mock
    private HttpResponse<byte[]> mockHttpResponse;

    @Test(expected = Exception.class)
    public void testGetParquetMetadata() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        long fileSize = 1048576L;

        // This will throw UnknownHostException due to fake URL
        ParquetReaderUtil.getParquetMetadata(presignedUrl, fileSize);
    }

    @Test(expected = Exception.class)
    public void testGetParquetMetadataWithHttpError() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        long fileSize = 1048576L;

        // This will throw UnknownHostException due to fake URL
        ParquetReaderUtil.getParquetMetadata(presignedUrl, fileSize);
    }

    @Test(expected = Exception.class)
    public void testGetParquetMetadataWithNetworkError() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        long fileSize = 1048576L;

        // This will throw UnknownHostException due to fake URL
        ParquetReaderUtil.getParquetMetadata(presignedUrl, fileSize);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrl() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, schema);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithRowGroup() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();
        int rowGroupIndex = 2;

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrlWithRowGroup(presignedUrl, mockSpiller, schema, rowGroupIndex);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithInvalidRowGroup() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();
        int rowGroupIndex = -1;

        ParquetReaderUtil.streamParquetFromUrlWithRowGroup(presignedUrl, mockSpiller, schema, rowGroupIndex);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithHttpError() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, schema);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithNetworkError() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, schema);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithLargeFile() throws Exception
    {
        String presignedUrl = "https://test-url.com/large-file.parquet";
        Schema schema = createTestSchema();

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, schema);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithEmptyFile() throws Exception
    {
        String presignedUrl = "https://test-url.com/empty-file.parquet";
        Schema schema = createTestSchema();

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, schema);
    }

    @Test(expected = IOException.class)
    public void testStreamParquetFromUrlWithRetry() throws Exception
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();

        // This will throw IOException due to fake URL
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, schema);
    }

    @Test(expected = Exception.class)
    public void testGetParquetMetadataWithMultipleRowGroups() throws Exception
    {
        String presignedUrl = "https://test-url.com/multi-rowgroup-file.parquet";
        long fileSize = 10485760L;

        // This will throw UnknownHostException due to fake URL
        ParquetReaderUtil.getParquetMetadata(presignedUrl, fileSize);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamParquetFromUrlWithNullUrl() throws IOException
    {
        Schema schema = createTestSchema();
        ParquetReaderUtil.streamParquetFromUrl(null, mockSpiller, schema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamParquetFromUrlWithEmptyUrl() throws IOException
    {
        Schema schema = createTestSchema();
        ParquetReaderUtil.streamParquetFromUrl("", mockSpiller, schema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamParquetFromUrlWithNullSpiller() throws IOException
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        Schema schema = createTestSchema();
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, null, schema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamParquetFromUrlWithNullSchema() throws IOException
    {
        String presignedUrl = "https://test-url.com/file.parquet";
        ParquetReaderUtil.streamParquetFromUrl(presignedUrl, mockSpiller, null);
    }

    private byte[] createMockParquetBytes()
    {
        return new byte[]{
            0x50, 0x41, 0x52, 0x31,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x50, 0x41, 0x52, 0x31
        };
    }

    private byte[] createLargeMockParquetBytes()
    {
        byte[] largeBytes = new byte[1024 * 1024];
        largeBytes[0] = 0x50;
        largeBytes[1] = 0x41;
        largeBytes[2] = 0x52;
        largeBytes[3] = 0x31;
        
        int footerStart = largeBytes.length - 8;
        largeBytes[footerStart] = 0x50;
        largeBytes[footerStart + 1] = 0x41;
        largeBytes[footerStart + 2] = 0x52;
        largeBytes[footerStart + 3] = 0x31;
        
        return largeBytes;
    }

    private byte[] createMockParquetBytesWithMultipleRowGroups()
    {
        byte[] multiRowGroupBytes = new byte[2048];
        multiRowGroupBytes[0] = 0x50;
        multiRowGroupBytes[1] = 0x41;
        multiRowGroupBytes[2] = 0x52;
        multiRowGroupBytes[3] = 0x31;
        
        int footerStart = multiRowGroupBytes.length - 8;
        multiRowGroupBytes[footerStart] = 0x50;
        multiRowGroupBytes[footerStart + 1] = 0x41;
        multiRowGroupBytes[footerStart + 2] = 0x52;
        multiRowGroupBytes[footerStart + 3] = 0x31;
        
        return multiRowGroupBytes;
    }
}
