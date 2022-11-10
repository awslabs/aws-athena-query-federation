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
package com.amazonaws.athena.storage;

import com.amazonaws.athena.storage.common.StorageObject;

import java.util.ArrayList;
import java.util.List;

public class TableListResult
{
    private final List<StorageObject> tables = new ArrayList<>();
    private String nextToken;

    private TableListResult()
    {
    }

    /**
     * Constructor to instantiate TableListResult with list of tables (maybe partial) and token to list table from the next page
     *
     * @param tables    List of tables found in current page from Google Cloud Storage
     * @param nextToken Next token to list tables from the next page
     */
    public TableListResult(List<StorageObject> tables, String nextToken)
    {
        this.tables.addAll(tables);
        this.nextToken = nextToken;
    }

    // Getters/Setters
    public List<StorageObject> getTables()
    {
        return new ArrayList<>(tables);
    }

    public void setTables(List<StorageObject> tables)
    {
        this.tables.addAll(tables);
    }

    public String getNextToken()
    {
        return nextToken;
    }

    public void setNextToken(String nextToken)
    {
        this.nextToken = nextToken;
    }
}
