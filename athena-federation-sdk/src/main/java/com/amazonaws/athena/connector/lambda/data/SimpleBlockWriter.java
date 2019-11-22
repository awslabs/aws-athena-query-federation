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
package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;

/**
 * Used to write a single Block using the BlockWriter programming model.
 *
 * @see BlockWriter
 */
public class SimpleBlockWriter
        implements BlockWriter
{
    private final Block block;

    /**
     * Basic constructor using a pre-allocated Block.
     *
     * @param block The Block to write into.
     */
    public SimpleBlockWriter(Block block)
    {
        this.block = block;
    }

    /**
     * Used to write rows into the Block that is managed by this BlockWriter.
     *
     * @param rowWriter The RowWriter that the BlockWriter should use to write rows into the Block(s) it is managing.
     * @See BlockWriter
     */
    public void writeRows(BlockWriter.RowWriter rowWriter)
    {
        int rowCount = block.getRowCount();

        int rows;
        try {
            rows = rowWriter.writeRows(block, rowCount);
        }
        catch (Exception ex) {
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }

        if (rows > 0) {
            block.setRowCount(rowCount + rows);
        }
    }

    /**
     * Provides access to the ConstraintEvaluator that will be applied to the generated Blocks.
     *
     * @See BlockWriter
     */
    @Override
    public ConstraintEvaluator getConstraintEvaluator()
    {
        return block.getConstraintEvaluator();
    }
}
