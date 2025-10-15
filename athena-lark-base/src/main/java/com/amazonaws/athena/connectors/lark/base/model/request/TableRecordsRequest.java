/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.lark.base.model.request;

import static java.util.Objects.requireNonNull;

/**
 * Request parameters for fetching table records from Lark Base.
 * Encapsulates all parameters needed to query records via the Search API.
 */
public final class TableRecordsRequest
{
    private final String baseId;
    private final String tableId;
    private final long pageSize;
    private final String pageToken;
    private final String filterJson;
    private final String sortJson;

    private TableRecordsRequest(Builder builder)
    {
        this.baseId = requireNonNull(builder.baseId, "baseId cannot be null");
        this.tableId = requireNonNull(builder.tableId, "tableId cannot be null");
        this.pageSize = builder.pageSize;
        this.pageToken = builder.pageToken;
        this.filterJson = builder.filterJson;
        this.sortJson = builder.sortJson;
    }

    public String getBaseId()
    {
        return baseId;
    }

    public String getTableId()
    {
        return tableId;
    }

    public long getPageSize()
    {
        return pageSize;
    }

    public String getPageToken()
    {
        return pageToken;
    }

    public String getFilterJson()
    {
        return filterJson;
    }

    public String getSortJson()
    {
        return sortJson;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String baseId;
        private String tableId;
        private long pageSize = 500;
        private String pageToken;
        private String filterJson;
        private String sortJson;

        private Builder()
        {
        }

        public Builder baseId(String baseId)
        {
            this.baseId = baseId;
            return this;
        }

        public Builder tableId(String tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder pageSize(long pageSize)
        {
            this.pageSize = pageSize;
            return this;
        }

        public Builder pageToken(String pageToken)
        {
            this.pageToken = pageToken;
            return this;
        }

        public Builder filterJson(String filterJson)
        {
            this.filterJson = filterJson;
            return this;
        }

        public Builder sortJson(String sortJson)
        {
            this.sortJson = sortJson;
            return this;
        }

        public TableRecordsRequest build()
        {
            return new TableRecordsRequest(this);
        }
    }
}
