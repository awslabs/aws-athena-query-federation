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
package com.amazonaws.connectors.athena.deltalake.protocol;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import io.findify.s3mock.S3Mock;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class DeltaTableStorageTest extends TestCase {

    @Test
    public void testGetLastCheckpointNoParts() {
        // Given
        DeltaTableSnapshotBuilder.CheckpointIdentifier checkpointIdentifier = new DeltaTableSnapshotBuilder.CheckpointIdentifier(210, 0, Optional.empty());
        List<String> expectedCheckpointFile = Collections.singletonList("00000000000000000210.checkpoint.parquet");
        // When
        List<String> checkpointFile = DeltaTableStorage.listCheckpointFiles(checkpointIdentifier);
        // Then
        assertEquals(checkpointFile, expectedCheckpointFile);
    }

    @Test
    public void testGetLastCheckpointWithParts() {
        // Given
        DeltaTableSnapshotBuilder.CheckpointIdentifier checkpointIdentifier = new DeltaTableSnapshotBuilder.CheckpointIdentifier(210, 0, Optional.of(3L));
        List<String> expectedCheckpointFile = Arrays.asList(
                "00000000000000000210.checkpoint.0000000001.0000000003.parquet",
                "00000000000000000210.checkpoint.0000000002.0000000003.parquet",
                "00000000000000000210.checkpoint.0000000003.0000000003.parquet"
        );
        // When
        List<String> checkpointFile = DeltaTableStorage.listCheckpointFiles(checkpointIdentifier);
        // Then
        assertEquals(checkpointFile, expectedCheckpointFile);
    }

}
