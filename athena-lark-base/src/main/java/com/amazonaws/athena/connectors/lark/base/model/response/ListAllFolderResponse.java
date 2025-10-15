/*-
 * #%L
 * glue-lark-base-crawler
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

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = ListAllFolderResponse.Builder.class)
public final class ListAllFolderResponse extends BaseResponse<ListAllFolderResponse.ListData>
{
    private ListAllFolderResponse(Builder builder)
    {
        super(builder);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public List<DriveFile> getFiles()
    {
        ListData data = getData();
        return data != null ? data.getFiles() : Collections.emptyList();
    }

    public String getNextPageToken()
    {
        ListData data = getData();
        return (data != null) ? data.getNextPageToken() : null;
    }

    public boolean hasMore()
    {
        ListData data = getData();
        return (data != null) && data.hasMore();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = DriveFile.Builder.class)
    public static final class DriveFile
    {
        private final String name;
        private final String parentToken;
        private final String token;
        private final String type;

        private DriveFile(Builder builder)
        {
            this.name = builder.name;
            this.parentToken = builder.parentToken;
            this.token = builder.token;
            this.type = builder.type;
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

        @SuppressWarnings("unused")
        @JsonProperty("parent_token")
        public String getParentToken()
        {
            return parentToken;
        }

        @JsonProperty("token")
        public String getToken()
        {
            return token;
        }

        @JsonProperty("type")
        public String getType()
        {
            return type;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static final class Builder
        {
            private String name;
            private String parentToken;
            private String token;
            private String type;

            private Builder()
            {
            }

            @JsonProperty("name")
            public Builder name(String name)
            {
                this.name = name;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("parent_token")
            public Builder parentToken(String parentToken)
            {
                this.parentToken = parentToken;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("token")
            public Builder token(String token)
            {
                this.token = token;
                return this;
            }

            @JsonProperty("type")
            public Builder type(String type)
            {
                this.type = type;
                return this;
            }

            public DriveFile build()
            {
                return new DriveFile(this);
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = ListData.Builder.class)
    public static final class ListData
    {
        private final List<DriveFile> files;
        private final String nextPageToken;
        private final boolean hasMore;

        private ListData(Builder builder)
        {
            this.files = builder.files != null ? List.copyOf(builder.files) : Collections.emptyList(); // Ensure immutable list
            this.nextPageToken = builder.nextPageToken;
            this.hasMore = builder.hasMore;
        }

        @JsonProperty("files")
        public List<DriveFile> getFiles()
        {
            return files;
        }

        @JsonProperty("next_page_token")
        public String getNextPageToken()
        {
            return nextPageToken;
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
            private List<DriveFile> files;
            private String nextPageToken;
            private boolean hasMore;

            private Builder()
            {
            }

            @SuppressWarnings("unused")
            @JsonProperty("files")
            public Builder files(List<DriveFile> files)
            {
                this.files = files;
                return this;
            }

            @SuppressWarnings("unused")
            @JsonProperty("next_page_token")
            public Builder nextPageToken(String nextPageToken)
            {
                this.nextPageToken = nextPageToken;
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
        public ListAllFolderResponse build()
        {
            return new ListAllFolderResponse(this);
        }

        @Override
        public Builder code(int code)
        {
            super.code(code);
            return this;
        }

        @Override
        public Builder msg(String msg)
        {
            super.msg(msg);
            return this;
        }

        @Override
        public Builder data(ListData data)
        {
            super.data(data);
            return this;
        }
    }
}
