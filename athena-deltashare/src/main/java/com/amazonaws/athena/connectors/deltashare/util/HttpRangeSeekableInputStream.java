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

import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

/**
 * Seekable input stream using HTTP range requests
 * Eliminates Lambda 512MB /tmp storage constraint
 */
public class HttpRangeSeekableInputStream extends SeekableInputStream 
{
    private static final Logger logger = LoggerFactory.getLogger(HttpRangeSeekableInputStream.class);
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 10000;
    private static final int READ_TIMEOUT_MS = 60000;
    
    private final String signedUrl;
    private final long fileSize;
    private long position;
    private InputStream currentStream;
    private HttpURLConnection currentConnection;
    private long streamStartPosition;
    private long streamEndPosition;

    public HttpRangeSeekableInputStream(String signedUrl, long fileSize) 
    {
        this.signedUrl = signedUrl;
        this.fileSize = fileSize;
        this.position = 0;
        this.currentStream = null;
        this.currentConnection = null;
        this.streamStartPosition = -1;
        this.streamEndPosition = -1;
    }

    @Override
    public long getPos() throws IOException 
    {
        return position;
    }

    @Override
    public void seek(long newPos) throws IOException 
    {
        if (newPos < 0 || newPos >= fileSize) {
            throw new IOException("Invalid seek position: " + newPos + " (file size: " + fileSize + ")");
        }
        
        position = newPos;
        
        if (currentStream != null && (newPos < streamStartPosition || newPos > streamEndPosition)) {
            closeCurrentStream();
        }
    }

    @Override
    public int read() throws IOException 
    {
        ensureStreamAtPosition();
        
        if (currentStream == null) {
            return -1;
        }
        
        int byteRead = currentStream.read();
        if (byteRead != -1) {
            position++;
        } else {
            closeCurrentStream();
        }
        
        return byteRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException 
    {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        
        ensureStreamAtPosition();
        
        if (currentStream == null) {
            return -1;
        }
        
        long remainingInStream = streamEndPosition - position + 1;
        int actualLen = (int) Math.min(len, remainingInStream);
        
        int bytesRead = currentStream.read(b, off, actualLen);
        if (bytesRead > 0) {
            position += bytesRead;
        }
        
        if (bytesRead == -1 || position > streamEndPosition) {
            closeCurrentStream();
        }
        
        return bytesRead;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException 
    {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException 
    {
        int offset = start;
        int remaining = len;
        
        while (remaining > 0) {
            int bytesRead = read(bytes, offset, remaining);
            if (bytesRead == -1) {
                throw new IOException("Unexpected EOF");
            }
            offset += bytesRead;
            remaining -= bytesRead;
        }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException 
    {
        if (!buf.hasRemaining()) {
            return 0;
        }
        
        byte[] temp = new byte[buf.remaining()];
        int bytesRead = read(temp, 0, temp.length);
        
        if (bytesRead > 0) {
            buf.put(temp, 0, bytesRead);
        }
        
        return bytesRead;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException 
    {
        while (buf.hasRemaining()) {
            int bytesRead = read(buf);
            if (bytesRead == -1) {
                throw new IOException("Unexpected EOF");
            }
        }
    }

    @Override
    public void close() throws IOException 
    {
        closeCurrentStream();
    }

    private void ensureStreamAtPosition() throws IOException 
    {
        if (position >= fileSize) {
            closeCurrentStream();
            return;
        }
        
        if (currentStream == null || position < streamStartPosition || position > streamEndPosition) {
            closeCurrentStream();
            openStreamAtPosition();
        }
    }

    private void openStreamAtPosition() throws IOException 
    {
        long startByte = position;
        long endByte = Math.min(position + BUFFER_SIZE - 1, fileSize - 1);
        
        try {
            URL url = new URL(signedUrl);
            currentConnection = (HttpURLConnection) url.openConnection();
            currentConnection.setRequestMethod("GET");
            currentConnection.setConnectTimeout(CONNECT_TIMEOUT_MS);
            currentConnection.setReadTimeout(READ_TIMEOUT_MS);
            currentConnection.setRequestProperty("Range", "bytes=" + startByte + "-" + endByte);
            
            int responseCode = currentConnection.getResponseCode();
            
            if (responseCode == HttpURLConnection.HTTP_PARTIAL) {
                currentStream = currentConnection.getInputStream();
                streamStartPosition = startByte;
                streamEndPosition = endByte;
            } else if (responseCode == HttpURLConnection.HTTP_OK) {
                logger.warn("Server doesn't support range requests, using full download");
                InputStream fullStream = currentConnection.getInputStream();
                
                long skipped = 0;
                while (skipped < startByte) {
                    long thisSkip = fullStream.skip(startByte - skipped);
                    if (thisSkip <= 0) {
                        fullStream.close();
                        currentConnection.disconnect();
                        throw new IOException("Unable to skip to position " + startByte);
                    }
                    skipped += thisSkip;
                }
                
                currentStream = fullStream;
                streamStartPosition = startByte;
                streamEndPosition = fileSize - 1;
            } else {
                currentConnection.disconnect();
                throw new IOException("HTTP request failed with response code: " + responseCode);
            }
            
        } catch (IOException e) {
            if (currentConnection != null) {
                currentConnection.disconnect();
                currentConnection = null;
            }
            throw new IOException("Failed to open HTTP range stream at position " + position, e);
        }
    }

    private void closeCurrentStream() 
    {
        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException e) {
                logger.warn("Error closing stream: {}", e.getMessage());
            }
            currentStream = null;
        }
        
        if (currentConnection != null) {
            currentConnection.disconnect();
            currentConnection = null;
        }
        
        streamStartPosition = -1;
        streamEndPosition = -1;
    }
}
