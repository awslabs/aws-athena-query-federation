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

import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for converting Parquet Int96 timestamp format to epoch milliseconds.
 */
public class DateTimeConverter 
{
    private static final Logger logger = LoggerFactory.getLogger(DateTimeConverter.class);
    
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588L;

    /**
     * Converts Int96 bytes to epoch milliseconds. Int96 format: 8 bytes nanoseconds + 4 bytes Julian day.
     */
    public static long convertInt96BytesToEpochMillis(byte[] bytes)
    {
        if (bytes.length != 12) {
            throw new IllegalArgumentException("Int96 binary data must be exactly 12 bytes, got " + bytes.length);
        }
        
        long nanos = 0;
        for (int i = 7; i >= 0; i--) {
            nanos = (nanos << 8) | (bytes[i] & 0xFF);
        }
        
        int julianDay = 0;
        for (int i = 11; i >= 8; i--) {
            julianDay = (julianDay << 8) | (bytes[i] & 0xFF);
        }
        
        long unixEpochDay = julianDay - JULIAN_EPOCH_OFFSET_DAYS;
        
        long epochMillis = unixEpochDay * 24L * 60 * 60 * 1000 + nanos / 1000000L;
        
        logger.info("Converted Int96 to epoch: Julian day {} -> Unix day {} -> {} ms", 
                    julianDay, unixEpochDay, epochMillis);
        
        return epochMillis;
    }

    /**
     * Converts Int96 binary data to epoch milliseconds (Binary wrapper).
     */
    public static long convertInt96BinaryToEpochMillis(Binary binary)
    {
        return convertInt96BytesToEpochMillis(binary.getBytes());
    }
}
