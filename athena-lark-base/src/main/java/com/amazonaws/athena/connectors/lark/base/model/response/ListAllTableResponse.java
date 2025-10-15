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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.model.response;

import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Collections;
import java.util.List;

/**
 * Response for List All Table
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ListAllTableResponse.Builder.class)
public final class ListAllTableResponse extends BaseResponse<ListAllTableResponse.ListData>
{
    private ListAllTableResponse(Builder builder)
    {
        super(builder);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public List<BaseItem> getItems()
    {
        ListData data = getData();
        return data != null ? data.getItems() : Collections.emptyList();
    }

    public String getPageToken()
    {
        ListData data = getData();
        return (data != null) ? data.getPageToken() : null;
    }

    public boolean hasMore()
    {
        ListData data = getData();
        return (data != null) && data.hasMore();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = BaseItem.Builder.class)
    public static final class BaseItem
    {
        private final String tableId;
        private final String revision;
        private final String name;

        private BaseItem(Builder builder)
        {
            this.tableId = builder.tableId;
            this.revision = builder.revision;
            this.name = builder.name;
        }

        @SuppressWarnings("unused")
        @JsonProperty("table_id")
        public String getTableId()
        {
            return tableId;
        }

        @SuppressWarnings("unused")
        @JsonProperty("revision")
        public String getRevision()
        {
            return revision;
        }

        @JsonProperty("name")
        public String getName()
        {
            return CommonUtil.sanitizeGlueRelatedName(name);
        }

        @SuppressWarnings("unused")
        public String getRawName()
        {
            return name;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private String tableId;
            private String revision;
            private String name;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("table_id")
            public Builder tableId(String tableId)
            {
                this.tableId = tableId;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("revision")
            public Builder revision(String revision)
            {
                this.revision = revision;
                return this;
            }

            @JsonProperty("name")
            public Builder name(String name)
            {
                this.name = name;
                return this;
            }

            public BaseItem build()
            {
                return new BaseItem(this);
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = ListData.Builder.class)
    public static final class ListData
    {
        private final List<BaseItem> items;
        private final String pageToken;
        private final boolean hasMore;

        private ListData(Builder builder)
        {
            this.items = builder.items != null ? List.copyOf(builder.items) : Collections.emptyList();
            this.pageToken = builder.pageToken;
            this.hasMore = builder.hasMore;
        }

        @JsonProperty("items")
        public List<BaseItem> getItems()
        {
            return items;
        }

        @JsonProperty("page_token")
        public String getPageToken()
        {
            return pageToken;
        }

        @JsonProperty("has_more")
        public boolean hasMore()
        {
            return hasMore;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static final class Builder
        {
            private List<BaseItem> items;
            private String pageToken;
            private boolean hasMore;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("items")
            public Builder items(List<BaseItem> items)
            {
                this.items = items;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("page_token")
            public Builder pageToken(String pageToken)
            {
                this.pageToken = pageToken;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("has_more")
            public Builder hasMore(boolean hasMore)
            {
                this.hasMore = hasMore;
                return this;
            }

            public ListData build()
            {
                return new ListData(this);
            }
        }
    }

    public static final class Builder extends BaseResponse.Builder<ListData>
    {
        private Builder()
        {
            super();
        }

        @Override
        public ListAllTableResponse build()
        {
            return new ListAllTableResponse(this);
        }
    }
}
