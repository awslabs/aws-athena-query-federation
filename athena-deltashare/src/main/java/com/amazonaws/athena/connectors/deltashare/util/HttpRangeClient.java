/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * HTTP client for range requests to presigned URLs. Optimized for Lambda storage constraints.
 */
public class HttpRangeClient 
{
    private static final Logger logger = LoggerFactory.getLogger(HttpRangeClient.class);
    private static final int CONNECT_TIMEOUT_MS = 10000;
    private static final int READ_TIMEOUT_MS = 60000;
    private static final int MAX_RETRIES = 3;

    /**
     * Gets file size using HTTP HEAD request with retry logic.
     */
    public static long getFileSize(String signedUrl) throws IOException 
    {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                logger.info("Getting file size from URL, attempt {}/{}", attempt, MAX_RETRIES);
                
                URL url = new URL(signedUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("HEAD");
                connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
                connection.setReadTimeout(READ_TIMEOUT_MS);
                
                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    long contentLength = connection.getContentLengthLong();
                    connection.disconnect();
                    
                    if (contentLength <= 0) {
                        throw new IOException("Invalid content length: " + contentLength);
                    }
                    
                    logger.info("File size determined: {} bytes", contentLength);
                    return contentLength;
                }
                
                connection.disconnect();
                throw new IOException("HTTP HEAD request failed with response code: " + responseCode);
                
            } catch (IOException e) {
                logger.warn("Attempt {}/{} failed: {}", attempt, MAX_RETRIES, e.getMessage());
                if (attempt == MAX_RETRIES) {
                    throw new IOException("Failed to get file size after " + MAX_RETRIES + " attempts", e);
                }
                
                try {
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
        
        throw new IOException("Unexpected end of retry loop");
    }

    /**
     * Fetches a byte range using HTTP Range request with retry logic.
     */
    public static byte[] fetchByteRange(String signedUrl, long startByte, long endByte) throws IOException 
    {
        if (startByte < 0 || endByte < startByte) {
            throw new IllegalArgumentException("Invalid byte range: " + startByte + "-" + endByte);
        }
        
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                logger.info("Fetching byte range {}-{}, attempt {}/{}", startByte, endByte, attempt, MAX_RETRIES);
                
                URL url = new URL(signedUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
                connection.setReadTimeout(READ_TIMEOUT_MS);
                connection.setRequestProperty("Range", "bytes=" + startByte + "-" + endByte);
                
                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_PARTIAL) {
                    return readFullyAndDisconnect(connection);
                } else if (responseCode == HttpURLConnection.HTTP_OK) {
                    logger.warn("Server doesn't support range requests, reading full response");
                    byte[] fullContent = readFullyAndDisconnect(connection);
                    
                    if (fullContent.length < endByte + 1) {
                        throw new IOException("File shorter than expected range");
                    }
                    
                    byte[] rangeContent = new byte[(int)(endByte - startByte + 1)];
                    System.arraycopy(fullContent, (int)startByte, rangeContent, 0, rangeContent.length);
                    return rangeContent;
                } else {
                    connection.disconnect();
                    throw new IOException("HTTP Range request failed with response code: " + responseCode);
                }
                
            } catch (IOException e) {
                logger.warn("Attempt {}/{} failed: {}", attempt, MAX_RETRIES, e.getMessage());
                if (attempt == MAX_RETRIES) {
                    throw new IOException("Failed to fetch byte range after " + MAX_RETRIES + " attempts", e);
                }
                
                try {
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
        
        throw new IOException("Unexpected end of retry loop");
    }

    private static byte[] readFullyAndDisconnect(HttpURLConnection connection) throws IOException 
    {
        try (InputStream inputStream = connection.getInputStream();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            
            return outputStream.toByteArray();
        } finally {
            connection.disconnect();
        }
    }
}
