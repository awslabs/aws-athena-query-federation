/*-
 * #%L
 * athena-influxdb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.influxdb;

import org.apache.arrow.vector.types.TimeUnit;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class InfluxDbRecordHandlerTest
{
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    public void testToZonedDateTimeNanoseconds()
    {
        final long nanos = 1782258710000000000L;
        assertEquals(Instant.ofEpochSecond(0L, nanos).atZone(UTC),
                InfluxDbRecordHandler.toZonedDateTime(nanos, TimeUnit.NANOSECOND));
    }

    @Test
    public void testToZonedDateTimeMilliseconds()
    {
        final long millis = 1782258710000L;
        assertEquals(Instant.ofEpochMilli(millis).atZone(UTC),
                InfluxDbRecordHandler.toZonedDateTime(millis, TimeUnit.MILLISECOND));
    }

    /**
     * The same instant expressed in each supported unit must decode identically —
     * this is exactly the ambiguity the magnitude heuristic could not guarantee.
     */
    @Test
    public void testAllUnitsResolveToSameInstant()
    {
        final long seconds = 1782258710L;
        final ZonedDateTime expected = Instant.ofEpochSecond(seconds).atZone(UTC);
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds, TimeUnit.SECOND));
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds * 1_000L, TimeUnit.MILLISECOND));
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds * 1_000_000L, TimeUnit.MICROSECOND));
        assertEquals(expected, InfluxDbRecordHandler.toZonedDateTime(seconds * 1_000_000_000L, TimeUnit.NANOSECOND));
    }
}
