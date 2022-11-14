/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.gcs;

import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.net.URL;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Answers.RETURNS_SELF;
import static org.powermock.api.mockito.PowerMockito.mock;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({ParquetUtil.class, FileCacheFactory.class, StorageOptions.class, GoogleCredentials.class, FileMetaData.class})
public class ParquetUtilTest extends GcsTestBase
{


    @Test
    public void testGetSchema() throws Exception
    {
        StorageWithInputTest storageWithInput = mockStorageWithInputFile(BUCKET, PARQUET_FILE_4_STREAM);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(PARQUET_FILE_4_STREAM);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        Path path = new Path(parquetFile.getAbsolutePath());
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        ParquetFileReader reader = new ParquetFileReader(new Configuration(), path, footer);
        PowerMockito.whenNew(ParquetFileReader.class).withAnyArguments().thenReturn(reader);
        MessageType schema = ParquetUtil.getSchema(storageWithInput.getInputFile());
        assertNotNull(schema);

    }

    @Test(expected = NullPointerException.class)
    public void testCreateReader() throws Exception
    {
        org.apache.hadoop.fs.Path inputFile = mock(Path.class);
        FilterCompat.Filter filter = mock(FilterCompat.Filter.class);
        ParquetReader parquetReader = mock(ParquetReader.class);
        ParquetReader.Builder reader = mock(ParquetReader.Builder.class, RETURNS_SELF);
        MessageType type = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_array",
                        new GroupType(REPEATED, "bag", new PrimitiveType(OPTIONAL, INT32, "array_element"))));
        PowerMockito.when(reader.build()).thenReturn(parquetReader);
        ParquetReader<Group> schema = ParquetUtil.createReader(inputFile, type, filter);

    }
}
