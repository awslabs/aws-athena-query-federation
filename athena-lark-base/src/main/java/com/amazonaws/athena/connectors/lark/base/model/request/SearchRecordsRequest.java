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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;

/**
 * Request body for Lark Base Search Records API
 *
 * @see "https://open.larksuite.com/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-record/search"
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class SearchRecordsRequest
{
    @JsonProperty("page_size")
    private final Integer pageSize;

    @JsonProperty("page_token")
    private final String pageToken;

    @JsonProperty("filter")
    @JsonRawValue
    private final String filter;

    @JsonProperty("sort")
    @JsonRawValue
    private final String sort;

    private SearchRecordsRequest(Builder builder)
    {
        this.pageSize = builder.pageSize;
        this.pageToken = builder.pageToken;
        this.filter = builder.filter;
        this.sort = builder.sort;
    }

    public Integer getPageSize()
    {
        return pageSize;
    }

    public String getPageToken()
    {
        return pageToken;
    }

    public String getFilter()
    {
        return filter;
    }

    public String getSort()
    {
        return sort;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Integer pageSize;
        private String pageToken;
        private String filter;
        private String sort;

        private Builder()
        {
        }

        public Builder pageSize(Integer pageSize)
        {
            this.pageSize = pageSize;
            return this;
        }

        public Builder pageToken(String pageToken)
        {
            this.pageToken = pageToken;
            return this;
        }

        public Builder filter(String filter)
        {
            this.filter = filter;
            return this;
        }

        public Builder sort(String sort)
        {
            this.sort = sort;
            return this;
        }

        public SearchRecordsRequest build()
        {
            return new SearchRecordsRequest(this);
        }
    }
}
