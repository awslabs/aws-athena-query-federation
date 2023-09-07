/*-
 * #%L
 * Amazon Athena GCS Connector
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static com.amazonaws.athena.connectors.gcs.GcsUtil.coerce;
import static org.junit.Assert.*;

public class GcsUtilsTest
{
    private RootAllocator allocator = null;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void tearDown() {
        allocator.close();
    }

    protected RootAllocator rootAllocator() {
        return allocator;
    }

    @Test
    public void testCreateUri()
    {
        String uri = GcsUtil.createUri("bucket", "test");
        assertEquals("gs://bucket/test", uri);
    }

    @Test
    public void testCreateUriPath()
    {
        String uri = GcsUtil.createUri("bucket/test");
        assertEquals("gs://bucket/test", uri);
    }

    @Test
    public void testCoercing()
    {
        Instant instant = Instant.now();
        long seconds = instant.getEpochSecond();
        long micros = java.util.concurrent.TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.MICRO_OF_SECOND);
        long nanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.NANO_OF_SECOND);
        long millis = java.util.concurrent.TimeUnit.SECONDS.toMillis(instant.getEpochSecond()) +
            instant.getLong(java.time.temporal.ChronoField.MILLI_OF_SECOND);

        ZonedDateTime zonedDateTime = instant.atZone(java.time.ZoneId.of("UTC"));
        LocalDateTime localDateTime = instant.atZone(java.time.ZoneId.of("UTC")).toLocalDateTime();

        // Test all of the types that are being coerced in StorageMetadata
        TimeStampNanoVector timestampNanoVector = new TimeStampNanoVector(Field.nullable("timestamp_nano_col", Types.MinorType.TIMESTAMPNANO.getType()), allocator);
        timestampNanoVector.allocateNew();
        timestampNanoVector.setSafe(0, nanos);
        timestampNanoVector.setValueCount(1);

        TimeStampSecVector timestampSecVector = new TimeStampSecVector(Field.nullable("timestamp_sec_col", Types.MinorType.TIMESTAMPSEC.getType()), allocator);
        timestampSecVector.allocateNew();
        timestampSecVector.setSafe(0, seconds);
        timestampSecVector.setValueCount(1);

        TimeStampMilliVector timestampMilliVector = new TimeStampMilliVector(Field.nullable("timestamp_milli_col", Types.MinorType.TIMESTAMPMILLI.getType()), allocator);
        timestampMilliVector.allocateNew();
        timestampMilliVector.setSafe(0, millis);
        timestampMilliVector.setValueCount(1);

        TimeMicroVector timeMicroVector = new TimeMicroVector(Field.nullable("timemicro_col", Types.MinorType.TIMEMICRO.getType()), allocator);
        timeMicroVector.allocateNew();
        timeMicroVector.setSafe(0, micros);
        timeMicroVector.setValueCount(1);

        TimeStampMicroVector timestampMicroVector = new TimeStampMicroVector(Field.nullable("timestamp_micro_col", Types.MinorType.TIMESTAMPMICRO.getType()), allocator);
        timestampMicroVector.allocateNew();
        timestampMicroVector.setSafe(0, micros);
        timestampMicroVector.setValueCount(1);

        TimeNanoVector timeNanoVector = new TimeNanoVector(Field.nullable("timenano_col", Types.MinorType.TIMENANO.getType()), allocator);
        timeNanoVector.allocateNew();
        timeNanoVector.setSafe(0, nanos);
        timeNanoVector.setValueCount(1);

        TimeStampMilliTZVector timeStampMilliTZVector = new TimeStampMilliTZVector(
            Field.nullable(
                "timestamp_millitz_col",
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
            allocator);
        timeStampMilliTZVector.allocateNew();
        timeStampMilliTZVector.setSafe(0, millis);
        timeStampMilliTZVector.setValueCount(1);

        TimeStampMicroTZVector timeStampMicroTZVector = new TimeStampMicroTZVector(
            Field.nullable(
                "timestamp_microtz_col",
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")),
            allocator);
        timeStampMicroTZVector.allocateNew();
        timeStampMicroTZVector.setSafe(0, micros);
        timeStampMicroTZVector.setValueCount(1);

        try {
            // test coercing
            // This will hit the Timestamp non long case (LocalDateTime)
            assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), coerce(timestampNanoVector, timestampNanoVector.getObject(0)));
            assertEquals(localDateTime.truncatedTo(ChronoUnit.SECONDS), coerce(timestampSecVector, timestampSecVector.getObject(0)));
            assertEquals(localDateTime.truncatedTo(ChronoUnit.MILLIS), coerce(timestampMilliVector, timestampMilliVector.getObject(0)));

            // This will hit the Time long case
            assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), coerce(timeMicroVector, timeMicroVector.getObject(0)));

            // This will hit the Timestamp non long case (LocalDateTime)
            assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), coerce(timestampMicroVector, timestampMicroVector.getObject(0)));

            // This will hit the Time long case
            assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), coerce(timeNanoVector, timeNanoVector.getObject(0)));

            // These cases will hit the Timestamp long case
            assertEquals(zonedDateTime.truncatedTo(ChronoUnit.MILLIS), coerce(timeStampMilliTZVector, timeStampMilliTZVector.getObject(0)));
            assertEquals(zonedDateTime.truncatedTo(ChronoUnit.MICROS), coerce(timeStampMicroTZVector, timeStampMicroTZVector.getObject(0)));
        }
        finally {
            // Close all the vectors
            VectorSchemaRoot.of(
                timestampNanoVector,
                timestampSecVector,
                timestampMilliVector,
                timeMicroVector,
                timestampMicroVector,
                timeNanoVector,
                timeStampMilliTZVector,
                timeStampMicroTZVector).close();
        }
    }
}
