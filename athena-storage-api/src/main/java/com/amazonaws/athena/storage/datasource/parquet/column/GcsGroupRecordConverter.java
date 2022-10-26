/*-
 * #%L
 * Amazon Athena Storage API
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.amazonaws.athena.storage.datasource.parquet.column;

import com.amazonaws.athena.storage.datasource.parquet.filter.ConstraintEvaluator;
import com.amazonaws.athena.storage.datasource.parquet.filter.ParquetConstraintEvaluator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class GcsGroupRecordConverter extends RecordMaterializer<Group>
{
    /**
     * Factory to create Group (as a record)
     */
    private final SimpleGroupFactory simpleGroupFactory;

    /**
     * Indicates a found to determine whether to assemble the record in-memory. Once it becomes false, it no longer assemble
     * records in-memory
     */
    private boolean matched = true;

    private final GcsSimpleGroupConverter root;

    /**
     * Constructs a row converter tha converts a row to a Group.
     *
     * @param schema    An instance of {@link org.apache.arrow.vector.types.pojo.Schema}
     * @param evaluator An instance of {@link ParquetConstraintEvaluator}
     */
    public GcsGroupRecordConverter(MessageType schema, ConstraintEvaluator evaluator)
    {
        this.simpleGroupFactory = new SimpleGroupFactory(schema);
        this.root = new GcsSimpleGroupConverter(null, 0, schema, evaluator)
        {
            @Override
            public void start()
            {
                GcsGroupRecordConverter.this.matched = true;
                this.current = simpleGroupFactory.newGroup();
            }

            @Override
            public void end()
            {
            }

            /**
             * Sets value to indicate a match.
             * @param isMatched Ture if a match found, false otherwise
             */
            @Override
            void matched(boolean isMatched)
            {
                matched &= isMatched;
            }

            /**
             * Checks to see if a match found
             *
             * @return Ture if a match found, false otherwise
             */
            @Override
            boolean matched()
            {
                return matched;
            }
        };
    }

    /**
     * Another process that finally assembles the records for spilling checks to see if it should skip the current record
     *
     * @return True if no match found, false otherwise to final assembly of record for an instance of {@link Group}
     */
    public boolean shouldSkipCurrent()
    {
        return !this.matched;
    }

    /**
     * @return The current record if assembled, null otherwise.
     */
    @Override
    public Group getCurrentRecord()
    {
        return root.getCurrentRecord();
    }

    /**
     * @return The group converter that converts a row to an instance of {@link Group}
     */
    @Override
    public GroupConverter getRootConverter()
    {
        return root;
    }
}
