/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs.common;

import java.util.List;

public class PartitionFolder
{
    private String folderPath;
    private List<StoragePartition> partitions;

    public PartitionFolder()
    {
    }

    public PartitionFolder(String folderPath, List<StoragePartition> partitions)
    {
        this.folderPath = folderPath;
        this.partitions = partitions;
    }

    public String getFolderPath()
    {
        return folderPath;
    }

    public void setFolderPath(String folderPath)
    {
        this.folderPath = folderPath;
    }

    public List<StoragePartition> getPartitions()
    {
        return partitions;
    }

    public void setPartitions(List<StoragePartition> partitions)
    {
        this.partitions = partitions;
    }

<<<<<<< HEAD
    @Override
    public String toString()
    {
        return "PartitionFolder{" +
                "folderPath='" + folderPath + '\'' +
                ", partitions=" + partitions +
                '}';
    }
=======
>>>>>>> 8913f0f9 (GcsMetadataHandler changes for doGetSplits)
}
