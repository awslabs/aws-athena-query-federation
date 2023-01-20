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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static com.amazonaws.athena.connectors.gcs.GcsUtil.coerce;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        // Build a schema for test
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(Field.nullable("timestamp_nano_col", Types.MinorType.TIMESTAMPNANO.getType()));
        schemaBuilder.addField(Field.nullable("nano_col", Types.MinorType.TIMENANO.getType()));
        schemaBuilder.addField(Field.nullable("timestamp_micro_col", Types.MinorType.TIMESTAMPNANO.getType()));
        schemaBuilder.addField(Field.nullable("micro_col", Types.MinorType.TIMENANO.getType()));
        Schema schema = schemaBuilder.build();
        // test data
        final int fieldCount = schema.getFields().size();
        Instant instant = Instant.now();
        long nanos = (instant.getEpochSecond() * 1_000_000_000L) + instant.getNano();
        LocalDateTime localDateTime = Instant.ofEpochMilli(nanos/1_000).atZone(ZoneId.systemDefault()).toLocalDateTime();
        // test coercing
        try (VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, rootAllocator())) {
            for (int i = 0; i < fieldCount; i++) {
                FieldVector vector = schemaRoot.getVector(i);
                Object value = null;
                switch (vector.getMinorType()) {
                    case TIMESTAMPNANO:
                        value = coerce(vector, localDateTime);
                        break;
                    case TIMENANO:
                        value = coerce(vector, new Date());
                        break;
                    case TIMEMICRO:
                        value = coerce(vector, coerce(vector, Instant.now().toEpochMilli()));
                        break;
                    case TIMESTAMPMICRO:
                        value = coerce(vector, localDateTime);
                        break;
                }
                assertNotNull("Coerced value is null", value);
            }
        }

    }
}
