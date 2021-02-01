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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
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
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))

                .addField("col2", new ArrowType.Decimal(38, 9))
                .addField("col3", new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"))
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

    @Test
    public void isNullRow()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Int(32, true))
                .addField("col3", new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"))
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

    @Test
    public void fieldToString()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"))
                .build();

        LocalDateTime ldt = LocalDateTime.of(2020, 03, 18, 12,54,29);

        //Make a block with 2 rows and no null rows
        Block block = allocator.createBlock(schema);
        BlockUtils.setValue(block.getFieldVector("col1"), 0, 10);
        BlockUtils.setValue(block.getFieldVector("col2"), 0, ldt);

        BlockUtils.setValue(block.getFieldVector("col1"), 1, 11);
        BlockUtils.setValue(block.getFieldVector("col2"), 1, ZonedDateTime.of(ldt, ZoneId.of("-05:00")));
        block.setRowCount(2);

        String expectedRows = "rows=2";
        String expectedCol1 = "[10, 11]";
        String expectedCol2 = "[2020-03-18T12:54:29Z[UTC], 2020-03-18T12:54:29-05:00]";
        String actual = block.toString();
        assertTrue(actual.contains(expectedRows));
        assertTrue(actual.contains(expectedCol1));
        assertTrue(actual.contains(expectedCol2));
    }

    @Test
    public void canSetDate()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Date(DateUnit.DAY))
                .build();

        Date date = LocalDate.parse("1998-1-1").toDate();

        Block block = allocator.createBlock(schema);
        BlockUtils.setValue(block.getFieldVector("col1"), 0, date);
    }
}
