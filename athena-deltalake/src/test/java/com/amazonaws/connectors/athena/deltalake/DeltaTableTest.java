/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.connectors.athena.deltalake.protocol.DeltaTable;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class DeltaTableTest {


    @Test
    public void testGetLastCheckpointNoParts() throws IOException {
        // Given
        DeltaTable.LastCheckpoint lastCheckpoint = new DeltaTable.LastCheckpoint(210, 0, Optional.empty());
        List<String> expectedCheckpointFile = Collections.singletonList("00000000000000000210.checkpoint.parquet");
        // When
        List<String> checkpointFile = DeltaTable.getCheckpointFiles(lastCheckpoint);
        // Then
        assertEquals(checkpointFile, expectedCheckpointFile);
    }

    @Test
    public void testGetLastCheckpointWithParts() throws IOException {
        // Given
        DeltaTable.LastCheckpoint lastCheckpoint = new DeltaTable.LastCheckpoint(210, 0, Optional.of(3L));
        List<String> expectedCheckpointFile = Arrays.asList(
            "00000000000000000210.checkpoint.0000000001.0000000003.parquet",
            "00000000000000000210.checkpoint.0000000002.0000000003.parquet",
            "00000000000000000210.checkpoint.0000000003.0000000003.parquet"
        );
        // When
        List<String> checkpointFile = DeltaTable.getCheckpointFiles(lastCheckpoint);
        // Then
        assertEquals(checkpointFile, expectedCheckpointFile);
    }

}
