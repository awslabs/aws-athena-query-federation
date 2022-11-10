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
package com.amazonaws.athena.storage.gcs.cache;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class CustomGcsReadChannel implements ReadChannel
{
    private final RandomAccessFile accessFile;
    private final long fileLength;
    private final byte[] fileByes;
    private boolean readOnce = false;
    private boolean closed = false;

    public CustomGcsReadChannel(File tempFile) throws IOException
    {
        accessFile = new RandomAccessFile(tempFile, "r");
        fileLength = accessFile.length();
        fileByes = FileUtils.readFileToByteArray(tempFile);
    }

    @Override
    public void close()
    {
        try {
            accessFile.close();
            closed = true;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void seek(long l) throws IOException
    {
        accessFile.seek(l);
    }

    @Override
    public void setChunkSize(int i)
    {

    }

    @Override
    public RestorableState<ReadChannel> capture()
    {
        return null;
    }

    @Override
    public int read(ByteBuffer dst)
    {
        if (!readOnce) {
            dst.put(fileByes);
            readOnce = true;
            return (int) fileLength;
        }
        return -1;
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }
}
