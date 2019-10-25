package com.amazonaws.athena.connector.lambda.domain;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class ColumnMetadata
{
    private final String name;
    private final ColumnType type;
    private final String comment;
    private final String extraInfo;
    private final boolean hidden;

    public ColumnMetadata(@JsonProperty("name") String name,
            @JsonProperty("type") ColumnType type,
            @JsonProperty("comment") String comment,
            @JsonProperty("extraInfo") String extraInfo,
            @JsonProperty("hidden") boolean hidden)
    {
        if (name == null || name.isEmpty()) {
            throw new NullPointerException("name is null or empty");
        }
        if (type == null) {
            throw new NullPointerException("type is null");
        }

        this.name = name;
        this.type = type;
        this.comment = comment;
        this.extraInfo = extraInfo;
        this.hidden = hidden;
    }

    public String getName()
    {
        return name;
    }

    public ColumnType getType()
    {
        return type;
    }

    public String getComment()
    {
        return comment;
    }

    public String getExtraInfo()
    {
        return extraInfo;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("comment", comment)
                .add("extraInfo", extraInfo)
                .add("hidden", hidden)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        ColumnMetadata that = (ColumnMetadata) o;

        return Objects.equal(this.name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }
}
