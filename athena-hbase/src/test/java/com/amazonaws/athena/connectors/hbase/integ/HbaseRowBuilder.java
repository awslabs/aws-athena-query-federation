/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.integ;

import org.apache.hadoop.hbase.client.Put;

/**
 * This class constructs a row that can be inserted into an HBase table.
 */
public class HbaseRowBuilder
{
    private final Put row;

    /**
     * The constructor instantiates the row with the row designation (usually the row number).
     * @param rowDesignation The row number (e.g. "1").
     */
    public HbaseRowBuilder(String rowDesignation)
    {
        row = new Put(rowDesignation.getBytes());
    }

    /**
     * Adds a single column to the row.
     * @param familyName The name of the family.
     * @param columnName The name of the column.
     * @param columnValue The value of the column.
     * @return A row builder object.
     */
    public HbaseRowBuilder addColumn(String familyName, String columnName, String columnValue)
    {
        row.addColumn(familyName.getBytes(), columnName.getBytes(), columnValue.getBytes());
        return this;
    }

    /**
     * Returns the built row.
     * @return A Put (row) object.
     */
    public Put build()
    {
        return row;
    }
}
