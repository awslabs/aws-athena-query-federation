package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaSerDe;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class SchemaSerializationTest
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaSerializationTest.class);

    private final ObjectMapper objectMapper = ObjectMapperFactory.create(new BlockAllocatorImpl());

    @Test
    public void serializationTest()
            throws IOException
    {
        logger.info("serializationTest - enter");
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addMetadata("meta1", "meta-value-1");
        schemaBuilder.addMetadata("meta2", "meta-value-2");
        schemaBuilder.addField("intfield1", new ArrowType.Int(32, true));
        schemaBuilder.addField("doublefield2", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        schemaBuilder.addField("varcharfield3", new ArrowType.Utf8());
        Schema expectedSchema = schemaBuilder.build();

        SchemaSerDe serDe = new SchemaSerDe();
        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        serDe.serialize(expectedSchema, schemaOut);

        TestPojo expected = new TestPojo(expectedSchema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        objectMapper.writeValue(out, expected);
        TestPojo actual = objectMapper.readValue(new ByteArrayInputStream(out.toByteArray()), TestPojo.class);

        Schema actualSchema = actual.getSchema();
        logger.info("serializationTest - fields[{}]", actualSchema.getFields());
        logger.info("serializationTest - meta[{}]", actualSchema.getCustomMetadata());

        assertEquals(expectedSchema.getFields(), actualSchema.getFields());
        assertEquals(expectedSchema.getCustomMetadata(), actualSchema.getCustomMetadata());

        logger.info("serializationTest - exit");
    }
}
