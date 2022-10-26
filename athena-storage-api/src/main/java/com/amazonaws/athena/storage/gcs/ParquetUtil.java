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

import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class ParquetUtil
{
    private ParquetUtil()
    {
    }

    /**
     * Creates an instance of {@link ParquetFileReader} and read the Schema to return
     *
     * @param inputFile An instance of GCSInputFile, a subclass of {@link org.apache.parquet.io.InputFile}
     * @return An instance of {@link MessageType}
     * @throws IOException If occurs during reading the schema
     */
    public static MessageType getSchema(GcsInputFile inputFile) throws IOException
    {
        MessageType schema = null;
        try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().useRecordFilter(true).useStatsFilter(true).build())) {
            schema = reader.getFileMetaData().getSchema();
        }
        return schema;
    }

    /**
     * Creates an instance of {@link ParquetReader} for the given arguments
     *
     * @param file   An instance of {@link Path} to get the cached file
     * @param schema An instance of {@link MessageType}
     * @param filter An instance of {@link FilterCompat.Filter}
     * @return An instance of {@link ParquetReader}
     * @throws IOException If occurs during the read operation
     */
    public static ParquetReader<Group> createReader(Path file, MessageType schema, FilterCompat.Filter filter) throws IOException
    {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        return ParquetReader.builder(new GroupReadSupport(), file)
                .withConf(conf)
                .withFilter(filter)
                .build();
    }
}
