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
package com.amazonaws.athena.storage.gcs.io;

import com.amazonaws.athena.storage.gcs.SeekableGcsInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

public class GcsInputFile implements InputFile
{
    private final SeekableGcsInputStream inputStream;

    /**
     * Constructor to instantiate from the {@link StorageFile}. This is a subclass of {@link InputFile}
     *
     * @param storageFile An instance of {@link StorageFile}
     */
    public GcsInputFile(StorageFile storageFile) throws IOException
    {
        this.inputStream = new SeekableGcsInputStream(storageFile);
    }

    /**
     * @param storageFile fileCache An instance of {@link StorageFile}
     * @param fileLength  Length of the file in bytes
     */
    public GcsInputFile(StorageFile storageFile, long fileLength)
    {
        this.inputStream = new SeekableGcsInputStream(storageFile, fileLength);
    }

    /**
     * @return Length of the underlying file
     */
    @Override
    public long getLength()
    {
        return inputStream.getFileSize();
    }

    /**
     * @return The underlying input stream
     */
    @Override
    public SeekableInputStream newStream()
    {
        return inputStream;
    }
}
