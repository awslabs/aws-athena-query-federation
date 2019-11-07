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
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;

import java.util.List;

/**
 * Used to write blocks which may require chunking and optionally spilling via a secondary communication channel.
 */
public interface BlockSpiller
        extends BlockWriter
{
    /**
     * Indicates if any part of the response written thus far has been spilled.
     *
     * @return True if at least 1 block has spilled, False otherwise.
     */
    boolean spilled();

    /**
     * Provides access to the single buffered block in the event that spilled() is false.
     *
     * @return
     */
    Block getBlock();

    /**
     * Provides access to the manifest of SpillLocation(s) if spilled is true.
     *
     * @return
     */
    List<SpillLocation> getSpillLocations();

    /**
     * Frees any resources associated with the BlockSpiller.
     */
    void close();

    /**
     * Provides access to the ConstraintEvaluator that will be applied to the generated Blocks.
     */
    ConstraintEvaluator getConstraintEvaluator();
}
