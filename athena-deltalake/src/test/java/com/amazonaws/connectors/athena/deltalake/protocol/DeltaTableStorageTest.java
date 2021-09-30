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
