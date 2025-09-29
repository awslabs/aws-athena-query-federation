/*-
 * #%L
 * athena-deltashare
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.deltashare.util;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * InputFile implementation for presigned URLs using HTTP range requests
 * Eliminates Lambda 512MB /tmp storage constraint
 */
public class PresignedRangeInputFile implements InputFile 
{
    private static final Logger logger = LoggerFactory.getLogger(PresignedRangeInputFile.class);
    
    private final String signedUrl;
    private final long fileSize;

    public PresignedRangeInputFile(String signedUrl, long fileSize) 
    {
        this.signedUrl = signedUrl;
        this.fileSize = fileSize;
        
        logger.info("Created PresignedRangeInputFile for URL: {} (size: {} bytes)",
                    signedUrl.substring(0, Math.min(100, signedUrl.length())), fileSize);
    }

    /**
     * Get the signed URL for this file
     */
    public String getSignedUrl() 
    {
        return signedUrl;
    }

    @Override
    public long getLength() throws IOException 
    {
        return fileSize;
    }

    @Override
    public SeekableInputStream newStream() throws IOException 
    {
        logger.info("Creating new HttpRangeSeekableInputStream");
        return new HttpRangeSeekableInputStream(signedUrl, fileSize);
    }

    @Override
    public String toString() 
    {
        return "PresignedRangeInputFile{" +
                "url=" + signedUrl.substring(0, Math.min(100, signedUrl.length())) + "..." +
                ", size=" + fileSize +
                '}';
    }
}
