package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.reader.Float8Reader;
import org.apache.arrow.vector.complex.reader.IntReader;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.codec.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//TODO: Consider breaking up this test into 3 separate tests but the setup for the test would be error prone
//      Also having them condensed like this gives a good example for how to use Apache Arrow.
public class BlockTest
{
    private static final String ENTRIES = "entries";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    private static final Logger logger = LoggerFactory.getLogger(BlockTest.class);
    private BlockAllocatorImpl allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void constrainedBlockTest()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addIntField("col1")
                .addIntField("col2")
                .build();

        Block block = allocator.createBlock(schema);

        ValueSet col1Constraint = EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(), true, false)
                .add(10).build();
        Constraints constraints = new Constraints(Collections.singletonMap("col1", col1Constraint));
        try (ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(allocator, schema, constraints)) {
            block.constrain(constraintEvaluator);
            assertTrue(block.setValue("col1", 0, 10));
            assertTrue(block.offerValue("col1", 0, 10));
            assertFalse(block.setValue("col1", 0, 11));
            assertFalse(block.offerValue("col1", 0, 11));
            assertTrue(block.offerValue("unkown_col", 0, 10));
        }
    }

    //TODO: Break this into multiple smaller tests, probably primitive types vs. complex vs. nested complex
    //TODO: List of Lists
    //TODO: List of Structs
    @Test
    public void EndToEndBlockTest()
            throws Exception
    {
        BlockAllocatorImpl expectedAllocator = new BlockAllocatorImpl();

        int expectedRows = 200;

        Schema origSchema = generateTestSchema();
        Block expectedBlock = generateTestBlock(expectedAllocator, origSchema, expectedRows);

        RecordBatchSerDe expectSerDe = new RecordBatchSerDe(expectedAllocator);
        ByteArrayOutputStream blockOut = new ByteArrayOutputStream();
        ArrowRecordBatch expectedBatch = expectedBlock.getRecordBatch();
        expectSerDe.serialize(expectedBatch, blockOut);
        assertSerializationOverhead(blockOut);
        expectedBatch.close();
        expectedBlock.close();

        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        schemaSerDe.serialize(origSchema, schemaOut);
        Schema actualSchema = schemaSerDe.deserialize(new ByteArrayInputStream(schemaOut.toByteArray()));

        BlockAllocatorImpl actualAllocator = new BlockAllocatorImpl();
        RecordBatchSerDe actualSerDe = new RecordBatchSerDe(actualAllocator);
        ArrowRecordBatch batch = actualSerDe.deserialize(blockOut.toByteArray());

        /**
         * Generate and write the block
         */
        Block actualBlock = actualAllocator.createBlock(actualSchema);
        actualBlock.loadRecordBatch(batch);
        batch.close();

        for (int i = 0; i < actualBlock.getRowCount(); i++) {
            logger.info("EndToEndBlockTest: util {}", BlockUtils.rowToString(actualBlock, i));
        }

        assertEquals("Row count missmatch", expectedRows, actualBlock.getRowCount());
        int actualFieldCount = 1;
        for (Field next : actualBlock.getFields()) {
            FieldReader vector = actualBlock.getFieldReader(next.getName());
            switch (vector.getMinorType()) {
                case INT:
                    IntReader intVector = vector;
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        intVector.setPosition(i);
                        assertEquals(i * actualFieldCount * 3, intVector.readInteger().intValue());
                    }
                    break;
                case FLOAT8:
                    Float8Reader fVector = vector;
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        fVector.setPosition(i);
                        assertEquals(i * actualFieldCount * 1.1, fVector.readDouble().doubleValue(), .1);
                    }
                    break;
                case VARCHAR:
                    VarCharReader vVector = (VarCharReader) vector;
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        vVector.setPosition(i);
                        assertEquals(String.valueOf(i * actualFieldCount), vVector.readText().toString());
                    }
                    break;
                case STRUCT:
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        FieldReader effectiveVector = vector;
                        if (vector.getField().getName().equals("structFieldNested28")) {
                            //If this is our struct with a nested struct, then grab the nested struct and check it
                            effectiveVector = vector.reader("nestedStruct");
                        }
                        effectiveVector.setPosition(i);
                        assertEquals(" name: " + effectiveVector.getField().getName(), Long.valueOf(i), effectiveVector.reader("nestedBigInt").readLong());
                        assertEquals(String.valueOf(1000 + i), effectiveVector.reader("nestedString").readText().toString());
                    }
                    break;
                case LIST:
                    int actual = 0;
                    Field child = vector.getField().getChildren().get(0);
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        vector.setPosition(i);
                        int entryValues = 0;
                        while (vector.next()) {
                            if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.BIGINT) {
                                assertEquals(Long.valueOf(i + entryValues++), vector.reader().readLong());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.VARCHAR) {
                                assertEquals(String.valueOf(1000 + i + entryValues++), vector.reader().readText().toString());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.SMALLINT) {
                                entryValues++;
                                assertTrue((new Integer(i + 1)).shortValue() == vector.reader().readShort());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.INT) {
                                entryValues++;
                                assertTrue(i == vector.reader().readInteger());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.TINYINT) {
                                entryValues++;
                                assertTrue((byte) i == vector.reader().readByte());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.FLOAT4) {
                                entryValues++;
                                assertTrue(i * 1.0F == vector.reader().readFloat());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.FLOAT8) {
                                entryValues++;
                                assertTrue(i * 1.0D == vector.reader().readDouble());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.DECIMAL) {
                                entryValues++;
                                assertTrue(i * 100L == vector.reader().readBigDecimal().unscaledValue().longValue());
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.VARBINARY) {
                                entryValues++;
                                //no comparing
                            }
                            else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.BIT) {
                                entryValues++;
                                assertTrue((i % 2 == 1) == vector.reader().readBoolean());
                            }
                        }
                        if (entryValues > 0) {actual++;}
                    }

                    assertEquals("failed for " + vector.getField().getName(), actualBlock.getRowCount(), actual);
                    break;
                default:
                    //todo: add more types here.
            }
            actualFieldCount++;
        }

        /**
         * Now check that we can unset a row properly
         */
        BlockUtils.unsetRow(0, actualBlock);

        for (Field next : actualBlock.getFields()) {
            FieldReader vector = actualBlock.getFieldReader(next.getName());
            switch (vector.getMinorType()) {
                case DATEDAY:
                case DATEMILLI:
                case TINYINT:
                case UINT1:
                case SMALLINT:
                case UINT2:
                case UINT4:
                case INT:
                case UINT8:
                case BIGINT:
                case FLOAT4:
                case FLOAT8:
                case DECIMAL:
                case VARBINARY:
                case VARCHAR:
                case BIT:
                case STRUCT:
                case MAP:
                    vector.setPosition(0);
                    assertFalse("Failed for " + vector.getMinorType() + " " + next.getName(), vector.isSet());
                    break;
                case LIST:
                    //no supported for unsetRow(...) this is a TODO to see if its possible some other way
                    break;
                default:
                    throw new UnsupportedOperationException(next.getType().getTypeID() + " is not supported");
            }
            actualFieldCount++;
        }

        logger.info("EndToEndBlockTest: block size {}", actualAllocator.getUsage());
        actualBlock.close();
    }

    public static Schema generateTestSchema()
    {
        /**
         * Generate and write the schema
         */
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addMetadata("meta1", "meta-value-1");
        schemaBuilder.addMetadata("meta2", "meta-value-2");
        schemaBuilder.addField("intfield1", new ArrowType.Int(32, true));
        schemaBuilder.addField("doublefield2", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        schemaBuilder.addField("varcharfield3", new ArrowType.Utf8());

        schemaBuilder.addField("datemillifield4", new ArrowType.Date(DateUnit.MILLISECOND));
        schemaBuilder.addField("tinyintfield5", new ArrowType.Int(8, true));
        schemaBuilder.addField("uint1field6", new ArrowType.Int(8, false));
        schemaBuilder.addField("smallintfield7", new ArrowType.Int(16, true));
        schemaBuilder.addField("uint2field8", new ArrowType.Int(16, false));
        schemaBuilder.addField("datedayfield9", new ArrowType.Date(DateUnit.DAY));
        schemaBuilder.addField("uint4field10", new ArrowType.Int(32, false));
        schemaBuilder.addField("bigintfield11", new ArrowType.Int(64, true));
        schemaBuilder.addField("decimalfield12", new ArrowType.Decimal(10, 2));
        schemaBuilder.addField("floatfield13", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        schemaBuilder.addField("varbinaryfield14", new ArrowType.Binary());
        schemaBuilder.addField("bitfield15", new ArrowType.Bool());

        schemaBuilder.addListField("varcharlist16", Types.MinorType.VARCHAR.getType());
        schemaBuilder.addListField("intlist17", Types.MinorType.INT.getType());
        schemaBuilder.addListField("bigintlist18", Types.MinorType.BIGINT.getType());
        schemaBuilder.addListField("tinyintlist19", Types.MinorType.TINYINT.getType());
        schemaBuilder.addListField("smallintlist20", Types.MinorType.SMALLINT.getType());
        schemaBuilder.addListField("float4list21", Types.MinorType.FLOAT4.getType());
        schemaBuilder.addListField("float8list22", Types.MinorType.FLOAT8.getType());
        schemaBuilder.addListField("shortdeclist23", new ArrowType.Decimal(10, 2));
        schemaBuilder.addListField("londdeclist24", new ArrowType.Decimal(21, 2));
        schemaBuilder.addListField("varbinarylist25", Types.MinorType.VARBINARY.getType());
        schemaBuilder.addListField("bitlist26", Types.MinorType.BIT.getType());

        schemaBuilder.addStructField("structField27");
        schemaBuilder.addChildField("structField27", "nestedBigInt", Types.MinorType.BIGINT.getType());
        schemaBuilder.addChildField("structField27", "nestedString", Types.MinorType.VARCHAR.getType());
        schemaBuilder.addChildField("structField27", "tinyintcol", Types.MinorType.TINYINT.getType());
        schemaBuilder.addChildField("structField27", "smallintcol", Types.MinorType.SMALLINT.getType());
        schemaBuilder.addChildField("structField27", "float4Col", Types.MinorType.FLOAT4.getType());
        schemaBuilder.addChildField("structField27", "float8Col", Types.MinorType.FLOAT8.getType());
        schemaBuilder.addChildField("structField27", "shortDecCol", new ArrowType.Decimal(10, 2));
        schemaBuilder.addChildField("structField27", "longDecCol", new ArrowType.Decimal(21, 2));
        schemaBuilder.addChildField("structField27", "binaryCol", Types.MinorType.VARBINARY.getType());
        schemaBuilder.addChildField("structField27", "bitCol", Types.MinorType.BIT.getType());
        schemaBuilder.addStructField("structFieldNested28");
        schemaBuilder.addChildField("structFieldNested28", "bitCol", Types.MinorType.BIT.getType());
        schemaBuilder.addChildField("structFieldNested28",
                FieldBuilder.newBuilder("nestedStruct", new ArrowType.Struct())
                        .addField("nestedString", Types.MinorType.VARCHAR.getType(), null)
                        .addField("nestedBigInt", Types.MinorType.BIGINT.getType(), null)
                        .addListField("nestedList", Types.MinorType.VARCHAR.getType())
                        .addListField("nestedListDec", new ArrowType.Decimal(10, 2))
                        .build());

        schemaBuilder.addField(FieldBuilder.newBuilder("simplemap", new ArrowType.Map(false))
            .addField("entries", Types.MinorType.STRUCT.getType(), false, Arrays.asList(
                    FieldBuilder.newBuilder("key", Types.MinorType.VARCHAR.getType(), false).build(),
                    FieldBuilder.newBuilder("value", Types.MinorType.INT.getType()).build()))
            .build());
        return schemaBuilder.build();
    }

    public static Block generateTestBlock(BlockAllocatorImpl expectedAllocator, Schema origSchema, int expectedRows)
            throws UnsupportedOperationException
    {
        /**
         * Generate and write the block
         */
        Block expectedBlock = expectedAllocator.createBlock(origSchema);
        int fieldCount = 1;
        for (Field next : origSchema.getFields()) {
            ValueVector vector = expectedBlock.getFieldVector(next.getName());
            switch (vector.getMinorType()) {
                case DATEDAY:
                    DateDayVector dateDayVector = (DateDayVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        dateDayVector.setSafe(i, i * fieldCount);
                    }
                    break;
                case UINT4:
                    UInt4Vector uInt4Vector = (UInt4Vector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        uInt4Vector.setSafe(i, i * fieldCount * 2);
                    }
                    break;
                case INT:
                    IntVector intVector = (IntVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        intVector.setSafe(i, i * fieldCount * 3);
                    }
                    break;
                case FLOAT8:
                    Float8Vector fVector = (Float8Vector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        fVector.setSafe(i, i * fieldCount * 1.1);
                    }
                    break;
                case VARCHAR:
                    VarCharVector vVector = (VarCharVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        vVector.setSafe(i, String.valueOf(i * fieldCount).getBytes(Charsets.UTF_8));
                    }
                    break;
                case DATEMILLI:
                    DateMilliVector dateMilliVector = (DateMilliVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        dateMilliVector.setSafe(i, i * fieldCount * 4);
                    }
                    break;
                case TINYINT:
                    TinyIntVector tinyIntVector = (TinyIntVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        tinyIntVector.setSafe(i, i * fieldCount * 5);
                    }
                    break;
                case UINT1:
                    UInt1Vector uInt1Vector = (UInt1Vector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        uInt1Vector.setSafe(i, i * fieldCount * 6);
                    }
                    break;
                case SMALLINT:
                    SmallIntVector smallIntVector = (SmallIntVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        smallIntVector.setSafe(i, i * fieldCount * 7);
                    }
                    break;
                case UINT2:
                    UInt2Vector uInt2Vector = (UInt2Vector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        uInt2Vector.setSafe(i, i * fieldCount * 8);
                    }
                    break;
                case UINT8:
                    UInt8Vector uInt8Vector = (UInt8Vector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        uInt8Vector.setSafe(i, i * fieldCount * 9);
                    }
                    break;
                case BIGINT:
                    BigIntVector bigIntVector = (BigIntVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        bigIntVector.setSafe(i, i * fieldCount * 10);
                    }
                    break;
                case DECIMAL:
                    DecimalVector decimalVector = (DecimalVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        BigDecimal bigDecimal = new BigDecimal((double) (i * fieldCount) * 1.01);
                        bigDecimal = bigDecimal.setScale(2, RoundingMode.HALF_UP);
                        decimalVector.setSafe(i, bigDecimal);
                    }
                    break;
                case FLOAT4:
                    Float4Vector float4Vector = (Float4Vector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        float4Vector.setSafe(i, i * fieldCount * 9);
                    }
                    break;
                case VARBINARY:
                    VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        byte[] data = String.valueOf(i * fieldCount).getBytes();
                        varBinaryVector.setSafe(i, data);
                    }
                    break;
                case BIT:
                    BitVector bitVector = (BitVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        bitVector.setSafe(i, i % 2);
                    }
                    break;
                case STRUCT:
                    StructVector sVector = (StructVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        final int seed = i;
                        BlockUtils.setComplexValue(sVector, i, (Field field, Object value) -> {
                            if (field.getName().equals("nestedBigInt")) {
                                return (long) seed;
                            }
                            if (field.getName().equals("nestedString")) {
                                return String.valueOf(1000 + seed);
                            }
                            if (field.getName().equals("tinyintcol")) {
                                return (byte) seed;
                            }

                            if (field.getName().equals("smallintcol")) {
                                return (short) seed;
                            }

                            if (field.getName().equals("nestedList")) {
                                List<String> values = new ArrayList<>();
                                values.add("val1");
                                values.add("val2");
                                return values;
                            }

                            if (field.getName().equals("nestedListDec")) {
                                List<Double> values = new ArrayList<>();
                                values.add(2.0D);
                                values.add(2.2D);
                                return values;
                            }

                            if (field.getName().equals("float4Col")) {
                                return seed * 1.0F;
                            }
                            if (field.getName().equals("float8Col")) {
                                return seed * 2.0D;
                            }
                            if (field.getName().equals("shortDecCol")) {
                                return seed * 3.0D;
                            }
                            if (field.getName().equals("longDecCol")) {
                                return seed * 4.0D;
                            }
                            if (field.getName().equals("binaryCol")) {
                                return String.valueOf(seed).getBytes(Charsets.UTF_8);
                            }
                            if (field.getName().equals("bitCol")) {
                                return seed % 2 == 1;
                            }
                            if (field.getName().equals("nestedStruct")) {
                                //doesn't matter since we are generating the values for the struct
                                //it just needs to be non-null
                                return new Object();
                            }

                            throw new RuntimeException("Unexpected field " + field.getName());
                        }, new Object());
                    }
                    break;
                case LIST:
                    Field child = vector.getField().getChildren().get(0);

                    if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.BIGINT) {
                        for (int i = 0; i < expectedRows; i++) {
                            List<Long> values = new ArrayList<>();
                            values.add(Long.valueOf(i));
                            values.add(i + 1L);
                            values.add(i + 2L);
                            BlockUtils.setComplexValue((ListVector) vector, i,
                                    FieldResolver.DEFAULT,
                                    values);
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.VARCHAR) {
                        for (int i = 0; i < expectedRows; i++) {
                            List<String> values = new ArrayList<>();
                            values.add(String.valueOf(1000 + i));
                            values.add(String.valueOf(1000 + i + 1));
                            values.add(String.valueOf(1000 + i + 2));
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    values);
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.SMALLINT) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList((short) (i + 1)));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.INT) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList(i));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.TINYINT) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList((byte) i));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.FLOAT4) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList((i * 1.0F)));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.FLOAT8) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList((i * 1.0D)));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.DECIMAL) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList((i * 1.0D)));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.VARBINARY) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList(String.valueOf(i).getBytes(Charsets.UTF_8)));
                        }
                    }
                    else if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.BIT) {
                        for (int i = 0; i < expectedRows; i++) {
                            BlockUtils.setComplexValue((ListVector) vector,
                                    i,
                                    FieldResolver.DEFAULT,
                                    Collections.singletonList(i % 2 == 1));
                        }
                    }
                    break;
                case MAP:
                    MapVector mapVector = (MapVector) vector;
                    for (int i = 0; i < expectedRows; i++) {
                        final int seed = i;
                        BlockUtils.setComplexValue(mapVector, i, (Field field, Object value) -> {
                            if (field.getName().equals("key")) {
                                return String.valueOf(1000 + seed);
                            }
                            if (field.getName().equals("value")) {
                                return seed;
                            }
                            if (field.getName().equals("tinyintcol")) {
                                return (byte) seed;
                            }

                            if (field.getName().equals("smallintcol")) {
                                return (short) seed;
                            }

                            if (field.getName().equals("nestedList")) {
                                List<String> values = new ArrayList<>();
                                values.add("val1");
                                values.add("val2");
                                return values;
                            }

                            if (field.getName().equals("nestedListDec")) {
                                List<Double> values = new ArrayList<>();
                                values.add(2.0D);
                                values.add(2.2D);
                                return values;
                            }

                            if (field.getName().equals("float4Col")) {
                                return seed * 1.0F;
                            }
                            if (field.getName().equals("float8Col")) {
                                return seed * 2.0D;
                            }
                            if (field.getName().equals("shortDecCol")) {
                                return seed * 3.0D;
                            }
                            if (field.getName().equals("longDecCol")) {
                                return seed * 4.0D;
                            }
                            if (field.getName().equals("binaryCol")) {
                                return String.valueOf(seed).getBytes(Charsets.UTF_8);
                            }
                            if (field.getName().equals("bitCol")) {
                                return seed % 2 == 1;
                            }
                            if (field.getName().equals("nestedStruct")) {
                                //doesn't matter since we are generating the values for the struct
                                //it just needs to be non-null
                                return new Object();
                            }

                            throw new RuntimeException("Unexpected field " + field.getName());
                        }, Map.of());
                    }
                    List<Field> children = vector.getField().getChildren();
                    Field keyValueStructField;
                    if (children.size() != 1) {
                        throw new IllegalStateException("Invalid Arrow Map schema: " + vector.getField());
                    }
                    else {
                        keyValueStructField = children.get(0);
                        if (!ENTRIES.equals(keyValueStructField.getName())
                                || !(keyValueStructField.getType() instanceof ArrowType.Struct)) {
                            throw new IllegalStateException("Invalid Arrow Map schema: " + vector.getField());
                        }
                    }

                    List<Field> keyValueChildren = keyValueStructField.getChildren();
                    Field keyField;
                    Field valueField;
                    if (keyValueChildren.size() != 2) {
                        throw new IllegalStateException("Invalid Arrow Map schema: " + vector.getField());
                    }
                    else {
                        keyField = keyValueChildren.get(0);
                        valueField = keyValueChildren.get(1);
                        if (!KEY.equals(keyField.getName()) || !VALUE.equals(valueField.getName())) {
                            throw new IllegalStateException("Invalid Arrow Map schema: " + vector.getField());
                        }
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(vector.getMinorType() + " is not supported");
            }
            fieldCount++;
        }
        expectedBlock.setRowCount(expectedRows);

        return expectedBlock;
    }

    @Test
    public void ListOfListsTest()
            throws Exception
    {
        BlockAllocatorImpl expectedAllocator = new BlockAllocatorImpl();

        /**
         * Generate and write the schema
         */
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addField(
                FieldBuilder.newBuilder("outerlist", new ArrowType.List())
                        .addListField("innerList", Types.MinorType.VARCHAR.getType())
                        .build());
        Schema origSchema = schemaBuilder.build();

        /**
         * Generate and write the block
         */
        Block expectedBlock = expectedAllocator.createBlock(origSchema);

        int expectedRows = 200;
        for (Field next : origSchema.getFields()) {
            ValueVector vector = expectedBlock.getFieldVector(next.getName());
            switch (vector.getMinorType()) {
                case LIST:
                    Field child = vector.getField().getChildren().get(0);
                    for (int i = 0; i < expectedRows; i++) {
                        //For each row
                        List<List<String>> value = new ArrayList<>();
                        switch (Types.getMinorTypeForArrowType(child.getType())) {
                            case LIST:
                                List<String> values = new ArrayList<>();
                                values.add(String.valueOf(1000));
                                values.add(String.valueOf(1001));
                                values.add(String.valueOf(1002));
                                value.add(values);
                                break;
                            default:
                                throw new UnsupportedOperationException(vector.getMinorType() + " is not supported");
                        }
                        BlockUtils.setComplexValue((ListVector) vector, i, FieldResolver.DEFAULT, value);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(vector.getMinorType() + " is not supported");
            }
        }
        expectedBlock.setRowCount(expectedRows);

        RecordBatchSerDe expectSerDe = new RecordBatchSerDe(expectedAllocator);
        ByteArrayOutputStream blockOut = new ByteArrayOutputStream();
        ArrowRecordBatch expectedBatch = expectedBlock.getRecordBatch();
        expectSerDe.serialize(expectedBatch, blockOut);
        assertSerializationOverhead(blockOut);
        expectedBatch.close();
        expectedBlock.close();

        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        schemaSerDe.serialize(origSchema, schemaOut);
        Schema actualSchema = schemaSerDe.deserialize(new ByteArrayInputStream(schemaOut.toByteArray()));

        BlockAllocatorImpl actualAllocator = new BlockAllocatorImpl();
        RecordBatchSerDe actualSerDe = new RecordBatchSerDe(actualAllocator);
        ArrowRecordBatch batch = actualSerDe.deserialize(blockOut.toByteArray());

        /**
         * Generate and write the block
         */
        Block actualBlock = actualAllocator.createBlock(actualSchema);
        actualBlock.loadRecordBatch(batch);
        batch.close();

        for (int i = 0; i < actualBlock.getRowCount(); i++) {
            logger.info("ListOfList: util {}", BlockUtils.rowToString(actualBlock, i));
        }

        assertEquals("Row count missmatch", expectedRows, actualBlock.getRowCount());
        int actualFieldCount = 1;
        for (Field next : actualBlock.getFields()) {
            FieldReader vector = actualBlock.getFieldReader(next.getName());
            switch (vector.getMinorType()) {
                case LIST:
                    int actual = 0;
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        vector.setPosition(i);
                        int entryValues = 0;
                        while (vector.next()) {
                            FieldReader innerReader = vector.reader();
                            int j = 0;
                            while (innerReader.next()) {
                                entryValues++;
                                assertEquals(String.valueOf(1000 + j++), innerReader.reader().readText().toString());
                            }
                        }
                        if (entryValues > 0) {actual++;}
                    }

                    assertEquals("failed for " + vector.getField().getName(), actualBlock.getRowCount(), actual);
                    break;
                default:
                    throw new UnsupportedOperationException(next.getType().getTypeID() + " is not supported");
            }
            actualFieldCount++;
        }

        actualBlock.close();
    }

    @Test
    public void ListOfStructsTest()
            throws Exception
    {
        BlockAllocatorImpl expectedAllocator = new BlockAllocatorImpl();

        /**
         * Generate and write the schema
         */
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addField(
                FieldBuilder.newBuilder("outerlist", new ArrowType.List())
                        .addField(
                                FieldBuilder.newBuilder("innerStruct", Types.MinorType.STRUCT.getType())
                                        .addStringField("varchar")
                                        .addBigIntField("bigint")
                                        .build())
                        .build());
        Schema origSchema = schemaBuilder.build();

        /**
         * Generate and write the block
         */
        Block expectedBlock = expectedAllocator.createBlock(origSchema);

        int expectedRows = 200;
        for (Field next : origSchema.getFields()) {
            ValueVector vector = expectedBlock.getFieldVector(next.getName());
            switch (vector.getMinorType()) {
                case LIST:
                    Field child = vector.getField().getChildren().get(0);
                    for (int i = 0; i < expectedRows; i++) {
                        //For each row
                        List<Map<String, Object>> value = new ArrayList<>();
                        switch (Types.getMinorTypeForArrowType(child.getType())) {
                            case STRUCT:
                                Map<String, Object> values = new HashMap<>();
                                values.put("varchar", "chars");
                                values.put("bigint", 100L);
                                value.add(values);
                                break;
                            default:
                                throw new UnsupportedOperationException(vector.getMinorType() + " is not supported");
                        }
                        BlockUtils.setComplexValue((ListVector) vector, i, FieldResolver.DEFAULT, value);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(vector.getMinorType() + " is not supported");
            }
        }
        expectedBlock.setRowCount(expectedRows);

        RecordBatchSerDe expectSerDe = new RecordBatchSerDe(expectedAllocator);
        ByteArrayOutputStream blockOut = new ByteArrayOutputStream();
        ArrowRecordBatch expectedBatch = expectedBlock.getRecordBatch();
        expectSerDe.serialize(expectedBatch, blockOut);
        assertSerializationOverhead(blockOut);
        expectedBatch.close();
        expectedBlock.close();

        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        schemaSerDe.serialize(origSchema, schemaOut);
        Schema actualSchema = schemaSerDe.deserialize(new ByteArrayInputStream(schemaOut.toByteArray()));

        BlockAllocatorImpl actualAllocator = new BlockAllocatorImpl();
        RecordBatchSerDe actualSerDe = new RecordBatchSerDe(actualAllocator);
        ArrowRecordBatch batch = actualSerDe.deserialize(blockOut.toByteArray());

        /**
         * Generate and write the block
         */
        Block actualBlock = actualAllocator.createBlock(actualSchema);
        actualBlock.loadRecordBatch(batch);
        batch.close();

        for (int i = 0; i < actualBlock.getRowCount(); i++) {
            logger.info("ListOfList: util {}", BlockUtils.rowToString(actualBlock, i));
        }

        assertEquals("Row count missmatch", expectedRows, actualBlock.getRowCount());
        int actualFieldCount = 1;
        for (Field next : actualBlock.getFields()) {
            FieldReader vector = actualBlock.getFieldReader(next.getName());
            switch (vector.getMinorType()) {
                case LIST:
                    int actual = 0;
                    for (int i = 0; i < actualBlock.getRowCount(); i++) {
                        vector.setPosition(i);
                        int entryValues = 0;
                        while (vector.next()) {
                            entryValues++;
                            assertEquals("chars", vector.reader().reader("varchar").readText().toString());
                            assertEquals(Long.valueOf(100), vector.reader().reader("bigint").readLong());
                        }
                        if (entryValues > 0) {actual++;}
                    }

                    assertEquals("failed for " + vector.getField().getName(), actualBlock.getRowCount(), actual);
                    break;
                default:
                    throw new UnsupportedOperationException(next.getType().getTypeID() + " is not supported");
            }
            actualFieldCount++;
        }

        actualBlock.close();
    }

    @Test
    public void structOfListsTest()
            throws Exception
    {
        BlockAllocatorImpl expectedAllocator = new BlockAllocatorImpl();

        /**
         * Generate and write the schema
         */
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addField(
                FieldBuilder.newBuilder("innerStruct", Types.MinorType.STRUCT.getType())
                        .addStringField("varchar")
                        .addListField("list", Types.MinorType.VARCHAR.getType())
                        .build());
        Schema origSchema = schemaBuilder.build();

        /**
         * Generate and write the block
         */
        Block expectedBlock = expectedAllocator.createBlock(origSchema);

        int expectedRows = 200;
        for (Field next : origSchema.getFields()) {
            ValueVector vector = expectedBlock.getFieldVector(next.getName());
            for (int i = 0; i < expectedRows; i++) {
                switch (vector.getMinorType()) {
                    case STRUCT:
                        Map<String, Object> value = new HashMap<>();
                        value.put("varchar", "chars");
                        if (i % 2 == 0) {
                            List<String> listVal = new ArrayList<>();
                            listVal.add("value_0_" + i);
                            listVal.add("value_1_" + i);
                            value.put("list", listVal);
                        }
                        else {
                            value.put("list", null);
                        }
                        BlockUtils.setComplexValue((StructVector) vector, i, FieldResolver.DEFAULT, value);
                        break;
                    default:
                        throw new UnsupportedOperationException(vector.getMinorType() + " is not supported");
                }
            }
        }
        expectedBlock.setRowCount(expectedRows);

        RecordBatchSerDe expectSerDe = new RecordBatchSerDe(expectedAllocator);
        ByteArrayOutputStream blockOut = new ByteArrayOutputStream();
        ArrowRecordBatch expectedBatch = expectedBlock.getRecordBatch();
        expectSerDe.serialize(expectedBatch, blockOut);

        assertSerializationOverhead(blockOut);
        expectedBatch.close();
        expectedBlock.close();

        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        SchemaSerDe schemaSerDe = new SchemaSerDe();
        schemaSerDe.serialize(origSchema, schemaOut);
        Schema actualSchema = schemaSerDe.deserialize(new ByteArrayInputStream(schemaOut.toByteArray()));

        BlockAllocatorImpl actualAllocator = new BlockAllocatorImpl();
        RecordBatchSerDe actualSerDe = new RecordBatchSerDe(actualAllocator);
        ArrowRecordBatch batch = actualSerDe.deserialize(blockOut.toByteArray());

        /**
         * Generate and write the block
         */
        Block actualBlock = actualAllocator.createBlock(actualSchema);
        actualBlock.loadRecordBatch(batch);
        batch.close();

        for (int i = 0; i < actualBlock.getRowCount(); i++) {
            logger.info("ListOfList: util {}", BlockUtils.rowToString(actualBlock, i));
        }

        assertEquals("Row count missmatch", expectedRows, actualBlock.getRowCount());
        int actualListValues = 0;
        int emptyListValues = 0;
        for (Field next : actualBlock.getFields()) {
            FieldReader vector = actualBlock.getFieldReader(next.getName());
            for (int i = 0; i < actualBlock.getRowCount(); i++) {
                switch (vector.getMinorType()) {
                    case STRUCT:
                        vector.setPosition(i);
                        assertEquals("chars", vector.reader("varchar").readText().toString());
                        FieldReader listReader = vector.reader("list");
                        int found = 0;
                        while (listReader.next()) {
                            assertEquals("value_" + found + "_" + i, listReader.reader().readText().toString());
                            found++;
                            actualListValues++;
                        }
                        if (found == 0) {
                            emptyListValues++;
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(next.getType().getTypeID() + " is not supported");
                }
            }
        }

        actualBlock.close();
        assertEquals(200, actualListValues);
        assertEquals(100, emptyListValues);
        logger.info("structOfListsTest: actualListValues[{}] emptyListValues[{}]", actualListValues, emptyListValues);
    }

    /**
     * Temporary 'HACK' - this assertion will fail if the overhead associated with serializing blocks exceeds
     * the hard coded expectation. This is only meaningful for inline blocks which will exceed Lambda's response size.
     * If this assertion fails we need to revisit the default settings in our serialization layer. The serialization
     * layer is currently being refactored and eventually this assertion will not be needed.
     *
     * @param serializedBlock The bytes of the block to serialize.
     * @note https://github.com/awslabs/aws-athena-query-federation/issues/121
     */
    private void assertSerializationOverhead(ByteArrayOutputStream serializedBlock)
    {
        try {
            ByteArrayOutputStream jout = new ByteArrayOutputStream();
            JsonFactory factory = new JsonFactory();
            JsonGenerator jsonGenerator = factory.createGenerator(jout);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeBinaryField("field", serializedBlock.toByteArray());
            jsonGenerator.close();
            double overhead = 1 - (((double) serializedBlock.size()) / ((double) jout.size()));
            logger.info("assertSerializationOverhead: {} vs {} = {}", serializedBlock.size(), jout.size(), overhead);
            assertTrue(0.35D > overhead);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
