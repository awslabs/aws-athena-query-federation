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
package com.amazonaws.athena.storage.datasource;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TypeFactoryTest
{
    @Test
    public void testTypeFieldResolver()
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.required(PrimitiveType.PrimitiveTypeName.INT64).named("id");
        builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named("count");
        builder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("factors");
        builder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("salary");
        builder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("onboarded");
        builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named("name");
        builder.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(32)
                .as(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.decimalType(2, 7)).named("bonus");
        builder.required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
                        .timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("created_at");
        builder.required(PrimitiveType.PrimitiveTypeName.INT96).named("last_modified");
        MessageType schema = builder.named("customer");
        FileMetaData fileMetaData = new FileMetaData(schema, Map.of(), null);
        ParquetMetadata parquetMetadata = mock(ParquetMetadata.class);
        when(parquetMetadata.getFileMetaData()).thenReturn(fileMetaData);
        TypeFactory.FieldResolver fieldResolver = TypeFactory.filedResolver(parquetMetadata);
        when(parquetMetadata.getFileMetaData()).thenAnswer((invocationOnMock) -> fileMetaData);
        List<Field> fields = fieldResolver.resolveFields();
        assertEquals("Field length in parquet schema and arrow schema weren't equal", schema.getColumns().size(), fields.size());
    }
}
