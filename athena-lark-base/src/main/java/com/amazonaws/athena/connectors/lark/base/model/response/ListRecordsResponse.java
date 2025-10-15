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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Response for List Records
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ListRecordsResponse.Builder.class)
public final class ListRecordsResponse extends BaseResponse<ListRecordsResponse.ListData>
{
    private ListRecordsResponse(Builder builder)
    {
        super(builder);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public List<RecordItem> getItems()
    {
        ListData data = getData();
        return data != null ? data.items() : Collections.emptyList();
    }

    public String getPageToken()
    {
        ListData data = getData();
        return (data != null) ? data.pageToken() : null;
    }

    public boolean hasMore()
    {
        ListData data = getData();
        return (data != null) && data.hasMore();
    }

    public int getTotal()
    {
        ListData data = getData();
        return (data != null) ? data.total() : 0;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = RecordItem.Builder.class)
    public static final class RecordItem
    {
        private Map<String, Object> fields;
        private final String recordId;

        private RecordItem(Builder builder)
        {
            // Ensure immutable map, filter out null values
            if (builder.fields != null) {
                Map<String, Object> nonNullFields = builder.fields.entrySet().stream()
                        .filter(e -> e.getValue() != null)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                this.fields = Map.copyOf(nonNullFields);
            }
            else {
                this.fields = Collections.emptyMap();
            }
            this.recordId = builder.recordId;
        }

        @JsonProperty("fields")
        public Map<String, Object> getFields()
        {
            return fields;
        }

        @JsonProperty("record_id")
        public String getRecordId()
        {
            return recordId;
        }

        public void setFields(Map<String, Object> fields)
        {
            this.fields = fields != null ? new HashMap<>(fields) : new HashMap<>();
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static final class Builder
        {
            private Map<String, Object> fields;
            private String recordId;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("fields")
            public Builder fields(Map<String, Object> fields)
            {
                this.fields = fields;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("record_id")
            public Builder recordId(String recordId)
            {
                this.recordId = recordId;
                return this;
            }

            public RecordItem build()
            {
                return new RecordItem(this);
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = ListData.Builder.class)
    public static final class ListData
    {
        private final List<RecordItem> items;
        private final String pageToken;
        private final boolean hasMore;
        private final int total;

        private ListData(Builder builder)
        {
            this.items = builder.items != null ? List.copyOf(builder.items) : Collections.emptyList();
            this.pageToken = builder.pageToken;
            this.hasMore = builder.hasMore;
            this.total = builder.total;
        }

        @JsonProperty("items")
        public List<RecordItem> items()
        {
            return items;
        }

        @JsonProperty("page_token")
        public String pageToken()
        {
            return pageToken;
        }

        @JsonProperty("has_more")
        public boolean hasMore()
        {
            return hasMore;
        }

        @JsonProperty("total")
        public int total()
        {
            return total;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private List<RecordItem> items;
            private String pageToken;
            private boolean hasMore;
            private int total;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("items")
            public Builder items(List<RecordItem> items)
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

            @SuppressWarnings("unused")
            @JsonProperty("total")
            public Builder total(int total)
            {
                this.total = total;
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
        public ListRecordsResponse build()
        {
            return new ListRecordsResponse(this);
        }
    }
}
