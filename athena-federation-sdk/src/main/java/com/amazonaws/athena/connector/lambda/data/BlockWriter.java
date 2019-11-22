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
 * Defines an abstraction that can be used to write to a Block without owning the lifecycle of the
 * Block.
 */
public interface BlockWriter
{
    /**
     * The interface you should implement for writing to a Block via
     * the inverted ownership model offered by BlockWriter.
     */
    interface RowWriter
    {
        /**
         * Used to accumulate rows as part of a block.
         *
         * @param block The block you can add your row to.
         * @param rowNum The row number in that block that the next row represents.
         * @return The number of rows that were added
         * @note We do not recommend writing more than 1 row per call. There are some use-cases which
         * are made much simpler by being able to write a small number (<100) rows per call. These often
         * relate to batched operators, scan side joins, or field expansions. Writing too many rows
         * will result in errors related to Block size management and are implementation specific.
         * @throws Exception internal exception.
         */
        int writeRows(Block block, int rowNum) throws Exception;
    }

    /**
     * Used to write rows via the BlockWriter.
     *
     * @param rowWriter The RowWriter that the BlockWriter should use to write rows into the Block(s) it is managing.
     */
    void writeRows(RowWriter rowWriter);

    /**
     * Provides access to the ConstraintEvaluator that will be applied to the generated Blocks.
     */
    ConstraintEvaluator getConstraintEvaluator();
}
