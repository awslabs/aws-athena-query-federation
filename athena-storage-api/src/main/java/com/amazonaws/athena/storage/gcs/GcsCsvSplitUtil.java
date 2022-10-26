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

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;

public class GcsCsvSplitUtil
{
    private GcsCsvSplitUtil()
    {
    }

    /**
     * Creates a list of {@link StorageSplit} instances
     *
     * @param fileName Name of the file in GCS
     * @param readSize Size of the records per split
     * @return A list of {@link StorageSplit} instances
     */
    public static synchronized List<StorageSplit> getStorageSplitList(long totalRecords, String fileName, int readSize)
    {
        int splitCount = (int) Math.ceil((double) totalRecords / readSize);
        int limit = toIntExact((splitCount == 0) ? totalRecords : readSize);
        long offSet = 0;
        int remainder = toIntExact(totalRecords % readSize);
        List<StorageSplit> storageSplitList = new ArrayList<>();
        for (int i = 1; i <= splitCount; i++) {
            if (i > 1) {
                offSet = offSet + limit;
            }
            if (remainder > 0 && i == splitCount) {
                storageSplitList.add(StorageSplit.builder()
                        .fileName(fileName)
                        .groupSplits(List.of(createSplit(offSet, remainder)))
                        .build());
            }
            else {
                storageSplitList.add(StorageSplit.builder()
                        .fileName(fileName)
                        .groupSplits(List.of(createSplit(offSet, limit)))
                        .build());
            }
        }
        return storageSplitList;
    }

    /**
     * Creates an instances of {@link GroupSplit} for the given arguments
     *
     * @param currentOffset Offset of the current record im the Split
     * @param rowsToRead    Size of the records to read
     * @return An instance of {@link GroupSplit}
     */
    private static synchronized GroupSplit createSplit(long currentOffset, long rowsToRead)
    {
        return GroupSplit.builder()
                .groupIndex(0)
                .rowOffset(currentOffset)
                .rowCount(rowsToRead)
                .build();
    }
}
