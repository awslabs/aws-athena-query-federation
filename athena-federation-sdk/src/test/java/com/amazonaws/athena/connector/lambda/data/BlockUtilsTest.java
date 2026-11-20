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

import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class BlockUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(BlockUtilsTest.class);

    private BlockAllocatorImpl allocator;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());
        // This is the default behavior of DateTimeFormatterUtil but some tests
        // will selectively disable it.
        DateTimeFormatterUtil.enableTimezonePacking();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void copyRows()
    {
        for (TimeUnit timeUnit : ImmutableList.of(TimeUnit.MILLISECOND, TimeUnit.MICROSECOND)) {
            if (timeUnit.equals(TimeUnit.MICROSECOND)) {
                DateTimeFormatterUtil.disableTimezonePacking();
            }
            Schema schema = SchemaBuilder.newBuilder()
                    .addField("col1", new ArrowType.Int(32, true))

                    .addField("col2", new ArrowType.Decimal(38, 9))
                    .addField("col3", new ArrowType.Timestamp(timeUnit, "UTC"))
                    .build();

            LocalDateTime ldt = LocalDateTime.of(2020, 03, 18, 12,54,29);
            Date date = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());

            //Make a block with 3 rows
            Block src = allocator.createBlock(schema);
            BlockUtils.setValue(src.getFieldVector("col1"), 0, 10);
            BlockUtils.setValue(src.getFieldVector("col2"), 0, new BigDecimal(20));
            BlockUtils.setValue(src.getFieldVector("col3"), 0, ldt);

            BlockUtils.setValue(src.getFieldVector("col1"), 1, 11);
            BlockUtils.setValue(src.getFieldVector("col2"), 1, new BigDecimal(21));
            BlockUtils.setValue(src.getFieldVector("col3"), 1, ZonedDateTime.of(ldt, ZoneId.of("-05:00")));

            BlockUtils.setValue(src.getFieldVector("col1"), 2, 12);
            BlockUtils.setValue(src.getFieldVector("col2"), 2, new BigDecimal(22));
            BlockUtils.setValue(src.getFieldVector("col3"), 2, date);
            src.setRowCount(3);

            //Make the destination block
            Block dst = allocator.createBlock(schema);

            assertEquals(3, BlockUtils.copyRows(src, dst, 0, 2));

            assertEquals(src, dst);
        }
    }

    @Test
    public void isNullRow()
    {
        for (TimeUnit timeUnit : ImmutableList.of(TimeUnit.MILLISECOND, TimeUnit.MICROSECOND)) {
            if (timeUnit.equals(TimeUnit.MICROSECOND)) {
                DateTimeFormatterUtil.disableTimezonePacking();
            }
            Schema schema = SchemaBuilder.newBuilder()
                    .addField("col1", new ArrowType.Int(32, true))
                    .addField("col2", new ArrowType.Int(32, true))
                    .addField("col3", new ArrowType.Timestamp(timeUnit, "UTC"))
                    .build();

            LocalDateTime ldt = LocalDateTime.now();

            //Make a block with 2 rows and no null rows
            Block block = allocator.createBlock(schema);
            BlockUtils.setValue(block.getFieldVector("col1"), 0, 10);
            BlockUtils.setValue(block.getFieldVector("col2"), 0, 20);
            BlockUtils.setValue(block.getFieldVector("col3"), 0, ldt);

            BlockUtils.setValue(block.getFieldVector("col1"), 1, 11);
            BlockUtils.setValue(block.getFieldVector("col2"), 1, 21);
            BlockUtils.setValue(block.getFieldVector("col3"), 1, ZonedDateTime.of(ldt, ZoneId.of("-05:00")));
            block.setRowCount(2);

            assertFalse(BlockUtils.isNullRow(block, 1));

            //now set a row to null
            BlockUtils.unsetRow(1, block);
            assertTrue(BlockUtils.isNullRow(block, 1));
        }
    }

    @Test
    public void fieldToString() throws java.text.ParseException
    {
        for (TimeUnit timeUnit : ImmutableList.of(TimeUnit.MILLISECOND, TimeUnit.MICROSECOND)) {
            String expectedString = "Block{rows=2, col1=[10, 11], col2=[2020-03-18T12:54:29Z[UTC], 2020-03-18T12:54:29-05:00], col3=[2020-03-18, 2019-12-29]}";
            if (timeUnit.equals(TimeUnit.MICROSECOND)) {
                DateTimeFormatterUtil.disableTimezonePacking();
                // The difference here is because TimeUnit.MILLISECOND with packing is using the packed
                // timezone value rather than using the timezone from the ArrowType
                expectedString = "Block{rows=2, col1=[10, 11], col2=[2020-03-18T12:54:29Z[UTC], 2020-03-18T17:54:29Z[UTC]], col3=[2020-03-18, 2019-12-29]}";
            }

            Schema schema = SchemaBuilder.newBuilder()
                    .addField("col1", new ArrowType.Int(32, true))
                    .addField("col2", new ArrowType.Timestamp(timeUnit, "UTC"))
                    .addField("col3", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY))
                    .build();

            LocalDateTime ldt = LocalDateTime.of(2020, 03, 18, 12,54,29);

            //Make a block with 2 rows and no null rows
            Block block = allocator.createBlock(schema);
            BlockUtils.setValue(block.getFieldVector("col1"), 0, 10);
            BlockUtils.setValue(block.getFieldVector("col2"), 0, ldt);
            BlockUtils.setValue(block.getFieldVector("col3"), 0, java.sql.Timestamp.valueOf(ldt));

            BlockUtils.setValue(block.getFieldVector("col1"), 1, 11);
            BlockUtils.setValue(block.getFieldVector("col2"), 1, ZonedDateTime.of(ldt, ZoneId.of("-05:00")));
            BlockUtils.setValue(block.getFieldVector("col3"), 1, new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2019-12-29"));
            block.setRowCount(2);

            String actual = block.toString();

            assertEquals(expectedString, actual);
        }
    }

    @Test
    public void canSetDate()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Date(DateUnit.DAY))
                .build();

        LocalDate date = LocalDate.parse("1998-01-01");

        Block block = allocator.createBlock(schema);
        BlockUtils.setValue(block.getFieldVector("col1"), 0, date);
    }

    /**
     * setValue accepts any Number for the integer and decimal vectors (some JDBC drivers hand back BigDecimal for
     * integer columns), but the conversion must be exact: in-range values are written as-is.
     */
    @Test
    public void setValueNumberConversionsInRange() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("tiny", Types.MinorType.TINYINT.getType())
                .addField("small", Types.MinorType.SMALLINT.getType())
                .addField("int", Types.MinorType.INT.getType())
                .addField("big", Types.MinorType.BIGINT.getType())
                .addField("dec", new ArrowType.Decimal(10, 2, 128))
                .build();
        try (Block block = allocator.createBlock(schema)) {
            BlockUtils.setValue(block.getFieldVector("tiny"), 0, new BigDecimal("-128"));
            BlockUtils.setValue(block.getFieldVector("small"), 0, 32767L);
            BlockUtils.setValue(block.getFieldVector("int"), 0, new BigDecimal("2147483647"));
            BlockUtils.setValue(block.getFieldVector("big"), 0, new BigDecimal("9223372036854775807"));
            BlockUtils.setValue(block.getFieldVector("big"), 1, new BigInteger("-9223372036854775808"));
            BlockUtils.setValue(block.getFieldVector("big"), 2, 42);
            BlockUtils.setValue(block.getFieldVector("big"), 3, 7.0d);
            BlockUtils.setValue(block.getFieldVector("dec"), 0, 5L);
            BlockUtils.setValue(block.getFieldVector("dec"), 1, 1.25d);
            block.setRowCount(4);

            assertEquals((byte) -128, ((TinyIntVector) block.getFieldVector("tiny")).get(0));
            assertEquals((short) 32767, ((SmallIntVector) block.getFieldVector("small")).get(0));
            assertEquals(2147483647, ((IntVector) block.getFieldVector("int")).get(0));
            assertEquals(9223372036854775807L, ((BigIntVector) block.getFieldVector("big")).get(0));
            assertEquals(-9223372036854775808L, ((BigIntVector) block.getFieldVector("big")).get(1));
            assertEquals(42L, ((BigIntVector) block.getFieldVector("big")).get(2));
            assertEquals(7L, ((BigIntVector) block.getFieldVector("big")).get(3));
            assertEquals(new BigDecimal("5.00"), ((DecimalVector) block.getFieldVector("dec")).getObject(0));
            assertEquals(new BigDecimal("1.25"), ((DecimalVector) block.getFieldVector("dec")).getObject(1));
        }
    }

    /**
     * A Number that does not fit the target vector (or has a fractional part) must fail loudly rather than be
     * written truncated or wrapped. The failure is surfaced as the SDK's AthenaConnectorException naming the field.
     */
    @Test
    public void setValueNumberConversionsOutOfRangeThrow() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("tiny", Types.MinorType.TINYINT.getType())
                .addField("small", Types.MinorType.SMALLINT.getType())
                .addField("int", Types.MinorType.INT.getType())
                .addField("big", Types.MinorType.BIGINT.getType())
                .build();
        try (Block block = allocator.createBlock(schema)) {
            assertSetValueThrows(block, "big", new BigDecimal("99999999999999999999"));   // 20 digits: wraps to 7766279631452241919 with longValue()
            assertSetValueThrows(block, "big", new BigInteger("9223372036854775808"));    // Long.MAX_VALUE + 1
            assertSetValueThrows(block, "big", new BigDecimal("1.5"));                    // fraction
            assertSetValueThrows(block, "int", new BigDecimal("3000000000.7"));           // wraps to -1294967296 with intValue()
            assertSetValueThrows(block, "int", 2147483648L);                              // Integer.MAX_VALUE + 1
            assertSetValueThrows(block, "int", 3.7d);                                     // truncates to 3 with intValue()
            assertSetValueThrows(block, "small", 32768);                                  // Short.MAX_VALUE + 1
            assertSetValueThrows(block, "tiny", 300);                                     // wraps to 44 with the byte narrowing
            assertSetValueThrows(block, "tiny", -129);
        }
    }

    private static void assertSetValueThrows(Block block, String field, Object value)
    {
        try {
            BlockUtils.setValue(block.getFieldVector(field), 0, value);
            fail("Expected setValue to reject " + value + " for " + field);
        }
        catch (com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("Unable to set value for field " + field));
        }
    }
}
