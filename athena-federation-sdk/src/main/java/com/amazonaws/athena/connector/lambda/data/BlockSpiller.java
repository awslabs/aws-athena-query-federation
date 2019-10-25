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

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;

import java.util.List;

public interface BlockSpiller
{
    interface RowWriter
    {
        /**
         * Used to accumulate rows as part of a response, with blocks being automatically spilled
         * as needed.
         *
         * @param block The block you can add your row to.
         * @param rowNum The row number in that block that the next row represents.
         * @return The number of rows that were added
         */
        int writeRows(Block block, int rowNum);
    }

    void writeRows(RowWriter rowWriter);

    boolean spilled();

    Block getBlock();

    List<SpillLocation> getSpillLocations();

    void close();
}
