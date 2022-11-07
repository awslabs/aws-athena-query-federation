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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Entry class to retrieve a DeltaTableSnapshot.
 * It contains all the Delta Log protocol logic.
 * It relies on a DeltaTableStorage to retrieve physical data.
 */
public class DeltaTableSnapshotBuilder
{
    DeltaTableStorage deltaTableStorage;

    public DeltaTableSnapshotBuilder(DeltaTableStorage deltaTableStorage)
    {
        this.deltaTableStorage = deltaTableStorage;
    }

    public static class DeltaTableSnapshot
    {
        public List<DeltaLogAction.AddFile> files;
        public DeltaLogAction.MetaData metaData;

        public DeltaTableSnapshot(List<DeltaLogAction.AddFile> files, DeltaLogAction.MetaData metaData)
        {
            this.files = files;
            this.metaData = metaData;
        }
    }

    public static class DeltaLogEntry
    {
        String fileName;
        List<DeltaLogAction> deltaLogActions;

        public DeltaLogEntry(String fileName, List<DeltaLogAction> deltaLogActions)
        {
            this.fileName = fileName;
            this.deltaLogActions = deltaLogActions;
        }
    }

    public static class Checkpoint
    {
        List<String> fileNames;
        List<DeltaLogAction> deltaLogActions;

        public Checkpoint(List<String> fileNames, List<DeltaLogAction> deltaLogActions)
        {
            this.fileNames = fileNames;
            this.deltaLogActions = deltaLogActions;
        }
    }

    public static class CheckpointIdentifier
    {
        long version;
        long size;
        Optional<Long> parts;

        public CheckpointIdentifier(long version, long size, Optional<Long> parts)
        {
            this.version = version;
            this.size = size;
            this.parts = parts;
        }
    }

    /**
     * Apply the Delta Log protocol reconciliation logic from a list of Delta log actions.
     * See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#action-reconciliation
     * @param deltaActions List of delta log actions
     * @return The Snapshot representing the Delta Table made of the input Delta actions
     */
    public static DeltaTableSnapshot reconciliateDeltaActions(List<DeltaLogAction> deltaActions)
    {
        Map<String, DeltaLogAction.AddFile> addFilesMap = new HashMap<>();
        DeltaLogAction.MetaData metaData = null;
        for (DeltaLogAction deltaAction : deltaActions) {
            if (deltaAction instanceof DeltaLogAction.AddFile) {
                DeltaLogAction.AddFile addFile = (DeltaLogAction.AddFile) deltaAction;
                addFilesMap.put(addFile.path, addFile);
            }
            else if (deltaAction instanceof DeltaLogAction.RemoveFile) {
                DeltaLogAction.RemoveFile removeFile = (DeltaLogAction.RemoveFile) deltaAction;
                addFilesMap.remove(removeFile.path);
            }
            else if (deltaAction instanceof DeltaLogAction.MetaData) {
                metaData = (DeltaLogAction.MetaData) deltaAction;
            }
        }
        return new DeltaTableSnapshot(new ArrayList<>(addFilesMap.values()), metaData);
    }

    /**
     * @return The current snapshot of the Delta table specified in deltaTableStorage.
     * @throws IOException
     */
    public DeltaTableSnapshot getSnapshot() throws IOException
    {
        CheckpointIdentifier lastCheckpointIdentifier = deltaTableStorage.getLastCheckpointIdentifier();
        List<DeltaLogAction> fullDeltaLogOrdered;
        if (lastCheckpointIdentifier != null) {
            Checkpoint lastCheckpoint = deltaTableStorage.getCheckpoint(lastCheckpointIdentifier);
            List<DeltaLogEntry> deltaLogs = deltaTableStorage.listDeltaLogsEntriesAfter(lastCheckpoint);
            fullDeltaLogOrdered = Stream.concat(
                    lastCheckpoint.deltaLogActions.stream(), deltaLogs.stream().flatMap(entry -> entry.deltaLogActions.stream()))
                    .collect(Collectors.toList());
        }
        else {
            fullDeltaLogOrdered = deltaTableStorage.listAllDeltaLogsEntries().stream()
                    .flatMap(entry -> entry.deltaLogActions.stream())
                    .collect(Collectors.toList());
        }
        return reconciliateDeltaActions(fullDeltaLogOrdered);
    }
}
