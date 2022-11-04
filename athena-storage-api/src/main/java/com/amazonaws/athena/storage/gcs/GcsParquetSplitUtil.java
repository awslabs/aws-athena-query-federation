/*-
 * #%L
 * athena-hive
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

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;

public class GcsParquetSplitUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsParquetSplitUtil.class);

    private static final String PARQUET_FILE_EXTENSION = ".parquet";
    private static final String CSV_FILE_EXTENSION = ".csv";
    private static final int MAX_CSV_READ_SIZE = 10_000;

    private GcsParquetSplitUtil()
    {
    }

    /**
     * Builds the storage split collection for a given parquet file
     *
     * @param fileName Name of the file from GCS
     * @param reader   An instance of {@link ParquetFileReader}
     * @param readSize Size of the records per split
     * @return A collection of {@link ParquetFileReader} instances
     */
    public static synchronized List<StorageSplit> getStorageSplitList(String fileName,
                                                                      ParquetFileReader reader,
                                                                      int readSize)
    {
        List<StorageSplit> storageSplitList = new ArrayList<>();
        readSize = maxReadSizeByFileType(fileName, readSize);
        long totalRecords = reader.getRecordCount();
        List<BlockMetaData> blockMetaDataList = reader.getRowGroups();
        int totalRowGroups = blockMetaDataList.size();
        LOGGER.debug("Total row groups {}", totalRowGroups);
        int splitCount = (int) Math.ceil((double) totalRecords / readSize);
        LOGGER.debug("Split count {}", splitCount);
        int groupIndex = 0;
        for (BlockMetaData block : blockMetaDataList) {
            addBlockStorageSplits(block, groupIndex++, readSize, fileName, storageSplitList);
        }
        return storageSplitList;
    }

    /**
     * Adds {@link StorageSplit} into the list
     *
     * @param block            An instance {@link com.amazonaws.athena.connector.lambda.data.Block}
     * @param groupIndex       Index of the group.
     * @param readSize         Total number of records to read per {@link GroupSplit}
     * @param fileName         Name of the remote file
     * @param storageSplitList List of {@link StorageSplit}
     */
    private static void addBlockStorageSplits(BlockMetaData block, int groupIndex, int readSize, String fileName,
                                              List<StorageSplit> storageSplitList)
    {
        long totalRecords = block.getRowCount();
        int splitCount = (int) Math.ceil((double) totalRecords / readSize);
        int limit = toIntExact((splitCount == 0) ? totalRecords : readSize);
        long offSet = 1;
        int remainder = toIntExact(totalRecords % readSize);
        for (int i = 1; i <= splitCount; i++) {
            if (i > 1) {
                offSet = offSet + limit;
            }
            if (remainder > 0 && i == splitCount) {
                storageSplitList.add(StorageSplit.builder()
                        .fileName(fileName)
                        .groupSplits(List.of(createSplit(groupIndex, offSet, remainder)))
                        .build());
            }
            else {
                storageSplitList.add(StorageSplit.builder()
                        .fileName(fileName)
                        .groupSplits(List.of(createSplit(groupIndex, offSet, limit)))
                        .build());
            }
        }
    }

    // helpers
    private static synchronized GroupSplit createSplit(int groupIndex, long currentOffset,
                                                       long rowsToRead)
    {
        return GroupSplit.builder()
                .groupIndex(groupIndex)
                .rowOffset(currentOffset)
                .rowCount(rowsToRead)
                .build();
    }

    /**
     * Determines maximum row read count per {@link GroupSplit} which is optimal for performance
     *
     * @param fileName Name of the file
     * @param readSize Total number of row to read per {@link GroupSplit}
     * @return Maximum read size (row count)
     */
    private static int maxReadSizeByFileType(String fileName, int readSize)
    {
        if (fileName.toLowerCase().endsWith(PARQUET_FILE_EXTENSION)) {
            return readSize;
        }
        else if (fileName.toLowerCase().endsWith(CSV_FILE_EXTENSION)) {
            return Math.min(MAX_CSV_READ_SIZE, readSize);
        }
        return readSize;
    }
}
