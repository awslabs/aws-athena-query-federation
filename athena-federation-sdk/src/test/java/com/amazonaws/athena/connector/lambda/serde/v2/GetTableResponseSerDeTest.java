/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.ProtoUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.fasterxml.jackson.core.JsonEncoding;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GetTableResponseSerDeTest extends TypedSerDeTest<FederationResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(GetTableResponseSerDeTest.class);

    @Before
    public void beforeTest()
            throws IOException
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("year", new ArrowType.Int(32, true))
                .addField("month", new ArrowType.Int(32, true))
                .addField("day", new ArrowType.Int(32, true))
                .addField("col3", new ArrowType.Utf8())
                .build();

        expected = new GetTableResponse(
                "test-catalog",
                new TableName("test-schema", "test-table"),
                schema,
                ImmutableSet.of("year", "month", "day"));

        String expectedSerDeFile = utils.getResourceOrFail("serde/v2", "GetTableResponse.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    /**
     * All relevant test cases for schema conversion.
     * 
     * Case 1. With a native Arrow Schema, convert to protobuf format, seriarlize it, and deserialize it. Does it match the native arrow schema?
     * Case 2. With a native Arrow Schema, serialize it with both jackson and with protobuf. Do they match each other?
     */
    @Test
    public void testProto_case1_nativeArrowToProtoToNativeArrow()
    {
        Schema expectedSchema = ((GetTableResponse) expected).getSchema();
        ByteString protoSchema = ProtoUtils.toProtoSchemaBytes(expectedSchema);
        logger.info(protoSchema.toString());
        
    }

    // TODO - either delete this or port it over to separate functionality. I was using this to debug why schema bytes weren't matching.
    // @Test
    // public void testProto_fromArrowSchemaToProtoAndBack() throws InvalidProtocolBufferException, IOException
    // {
    //     // Build GetTableResponse in proto from
    //     Schema expectedSchema = ((GetTableResponse) expected).getSchema();
    //     com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse protoGTR = com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse.newBuilder()
    //         .setType("GetTableResponse")
    //         .setCatalogName("test-catalog")
    //         .setTableName(ProtoUtils.toTableName(new TableName("test-schema", "test-table")))
    //         .setSchema(ProtoUtils.toProtoSchemaBytes(expectedSchema))
    //         .addAllPartitionColumns(List.of("year", "month", "day"))
    //         .build();
    //     logger.info("PRINTING RESPONSE IN PROTO FORMAT - {}", JsonFormat.printer().print(protoGTR));
    //     assertEquals(expectedSerDeText, JsonFormat.printer().print(protoGTR));
        

    //     // just test if we can deserialize a schema properly
    //     Schema backToSchema = ProtoUtils.fromProtoSchema(allocator, ProtoUtils.toProtoSchemaBytes(expectedSchema));
    //     assertEquals(expectedSchema, backToSchema);

    //     // compare...
    //     ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    //     mapper.writeValue(outputStream, expected);
    //     String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
    //     logger.info("serialized text for full repsonse, including schema, using jackson is [{}]", actual);
    // }

    @Test
    public void testProto_fromProtoToArrowSchemaAndBack()
    {
        
    }

    @Test
    public void serialize()
            throws IOException
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapper.writeValue(outputStream, expected);

        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize()
            throws IOException
    {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        GetTableResponse actual = (GetTableResponse) mapper.readValue(input, FederationResponse.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }
}
