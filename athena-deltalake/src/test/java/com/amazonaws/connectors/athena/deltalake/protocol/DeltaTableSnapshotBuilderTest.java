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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeltaTableSnapshotBuilderTest {

    DeltaTableSnapshotBuilder snapshotBuilder;

    @Test
    public void getSnapshotWithCheckpoint() throws IOException {
        // Given
        DeltaTableStorage mockTableStorage = Mockito.mock(DeltaTableStorage.class);
        snapshotBuilder = new DeltaTableSnapshotBuilder(mockTableStorage);
        DeltaTableSnapshotBuilder.CheckpointIdentifier lastCheckpointIdentifier =
                new DeltaTableSnapshotBuilder.CheckpointIdentifier(3, 100, Optional.empty());
        DeltaTableSnapshotBuilder.Checkpoint checkpoint =
                new DeltaTableSnapshotBuilder.Checkpoint(Arrays.asList("checkpoint"), Arrays.asList(
                        new DeltaLogAction.AddFile("path1", Collections.emptyMap()),
                        new DeltaLogAction.AddFile("path2", Collections.emptyMap()),
                        new DeltaLogAction.MetaData("schemaString1", Collections.emptyList()),
                        new DeltaLogAction.AddFile("path3", Collections.emptyMap())
                ));
        List<DeltaTableSnapshotBuilder.DeltaLogEntry> logEntries = Arrays.asList(
                new DeltaTableSnapshotBuilder.DeltaLogEntry("log1", Arrays.asList(
                        new DeltaLogAction.AddFile("path5", Collections.emptyMap()),
                        new DeltaLogAction.RemoveFile("path2")
                )),
                new DeltaTableSnapshotBuilder.DeltaLogEntry("log2", Arrays.asList(
                        new DeltaLogAction.RemoveFile("path5"),
                        new DeltaLogAction.AddFile("path6", Collections.emptyMap()),
                        new DeltaLogAction.MetaData("schemaString2", Collections.emptyList())
                ))
        );
        when(mockTableStorage.getLastCheckpointIdentifier()).thenReturn(lastCheckpointIdentifier);
        when(mockTableStorage.getCheckpoint(lastCheckpointIdentifier)).thenReturn(checkpoint);
        when(mockTableStorage.listDeltaLogsEntriesAfter(checkpoint)).thenReturn(logEntries);

        // When
        DeltaTableSnapshotBuilder.DeltaTableSnapshot snapshot = snapshotBuilder.getSnapshot();

        // Then
        assertEquals(3, snapshot.files.size());
        assertEquals("path1", snapshot.files.get(0).path);
        assertEquals("path3", snapshot.files.get(1).path);
        assertEquals("path6", snapshot.files.get(2).path);

        assertEquals("schemaString2", snapshot.metaData.schemaString);
    }


    @Test
    public void getSnapshotWithoutCheckpoint() throws IOException {
        // Given
        DeltaTableStorage mockTableStorage = Mockito.mock(DeltaTableStorage.class);
        snapshotBuilder = new DeltaTableSnapshotBuilder(mockTableStorage);
        List<DeltaTableSnapshotBuilder.DeltaLogEntry> logEntries = Arrays.asList(
                new DeltaTableSnapshotBuilder.DeltaLogEntry("log1", Arrays.asList(
                        new DeltaLogAction.AddFile("path5", Collections.emptyMap()),
                        new DeltaLogAction.RemoveFile("path2")
                )),
                new DeltaTableSnapshotBuilder.DeltaLogEntry("log2", Arrays.asList(
                        new DeltaLogAction.RemoveFile("path5"),
                        new DeltaLogAction.AddFile("path6", Collections.emptyMap()),
                        new DeltaLogAction.MetaData("schemaString1", Collections.emptyList())
                ))
        );
        when(mockTableStorage.getLastCheckpointIdentifier()).thenReturn(null);
        when(mockTableStorage.listAllDeltaLogsEntries()).thenReturn(logEntries);

        // When
        DeltaTableSnapshotBuilder.DeltaTableSnapshot snapshot = snapshotBuilder.getSnapshot();

        // Then
        assertEquals(1, snapshot.files.size());
        assertEquals("path6", snapshot.files.get(0).path);

        assertEquals("schemaString1", snapshot.metaData.schemaString);
    }


    @Test
    public void reconciliateDeltaActionsRemoveFile() {
        // Given
        List<DeltaLogAction> actions = Arrays.asList(
            new DeltaLogAction.AddFile("path1", Collections.emptyMap()),
            new DeltaLogAction.AddFile("path2", Collections.emptyMap()),
            new DeltaLogAction.RemoveFile("path1")
        );
        List<DeltaLogAction> expectedAddFiles = Arrays.asList(
                new DeltaLogAction.AddFile("path2", Collections.emptyMap())
        );

        // When
        DeltaTableSnapshotBuilder.DeltaTableSnapshot snapshot = DeltaTableSnapshotBuilder.reconciliateDeltaActions(actions);

        // Then
        assertEquals(expectedAddFiles, snapshot.files);
    }

    @Test
    public void reconciliateDeltaActionsRemoveThenReAddFile() {
        // Given
        List<DeltaLogAction> actions = Arrays.asList(
                new DeltaLogAction.AddFile("path1", Collections.emptyMap()),
                new DeltaLogAction.AddFile("path2", Collections.emptyMap()),
                new DeltaLogAction.RemoveFile("path1"),
                new DeltaLogAction.AddFile("path1", Collections.emptyMap())
        );
        List<DeltaLogAction> expectedAddFiles = Arrays.asList(
                new DeltaLogAction.AddFile("path1", Collections.emptyMap()),
                new DeltaLogAction.AddFile("path2", Collections.emptyMap())
        );

        // When
        DeltaTableSnapshotBuilder.DeltaTableSnapshot snapshot = DeltaTableSnapshotBuilder.reconciliateDeltaActions(actions);

        // Then
        assertEquals(expectedAddFiles, snapshot.files);
    }

    @Test
    public void reconciliateDeltaActionsMetaDataUpdate() {
        // Given
        List<DeltaLogAction> actions = Arrays.asList(
                new DeltaLogAction.AddFile("path1", Collections.emptyMap()),
                new DeltaLogAction.MetaData("schemaString1", Collections.emptyList()),
                new DeltaLogAction.MetaData("schemaString2", Arrays.asList("col1"))
        );
        DeltaLogAction.MetaData expectedMetadata = new DeltaLogAction.MetaData("schemaString2", Arrays.asList("col1"));

        // When
        DeltaTableSnapshotBuilder.DeltaTableSnapshot snapshot = DeltaTableSnapshotBuilder.reconciliateDeltaActions(actions);

        // Then
        assertEquals(expectedMetadata, snapshot.metaData);
    }
}
