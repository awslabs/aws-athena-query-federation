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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class BlockUtilsTest
{
    private BlockAllocatorImpl allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void copyRows()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Decimal(38, 9))
                .build();

        //Make a block with 3 rows
        Block src = allocator.createBlock(schema);
        BlockUtils.setValue(src.getFieldVector("col1"), 0, 10);
        BlockUtils.setValue(src.getFieldVector("col2"), 0, new BigDecimal(20));
        BlockUtils.setValue(src.getFieldVector("col1"), 1, 11);
        BlockUtils.setValue(src.getFieldVector("col2"), 1, new BigDecimal(21));
        BlockUtils.setValue(src.getFieldVector("col1"), 2, 12);
        BlockUtils.setValue(src.getFieldVector("col2"), 2, new BigDecimal(22));
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
                .build();

        //Make a block with 2 rows and no null rows
        Block block = allocator.createBlock(schema);
        BlockUtils.setValue(block.getFieldVector("col1"), 0, 10);
        BlockUtils.setValue(block.getFieldVector("col2"), 0, 20);
        BlockUtils.setValue(block.getFieldVector("col1"), 1, 11);
        BlockUtils.setValue(block.getFieldVector("col2"), 1, 21);
        block.setRowCount(2);

        assertFalse(BlockUtils.isNullRow(block, 1));

        //now set a row to null
        BlockUtils.unsetRow(1, block);
        assertTrue(BlockUtils.isNullRow(block, 1));
    }
}
