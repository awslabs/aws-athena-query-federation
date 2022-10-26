/*-
 * #%L
 * athena-hive
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
package com.amazonaws.athena.storage.gcs;

import com.fasterxml.jackson.annotation.JsonInclude;

import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class GroupSplit
{
    private int groupIndex;
    private Long rowOffset;
    private Long rowCount;

    // Jackson uses this constructor
    @SuppressWarnings("unused")
    public GroupSplit()
    {
    }

    /**
     * Constructor to instantiate with given arguments
     *
     * @param groupIndex Index of the group
     * @param rowOffset  Start offset of the record in this split
     * @param rowCount   Size of the records being read
     */
    public GroupSplit(int groupIndex, Long rowOffset, Long rowCount)
    {
        this.groupIndex = groupIndex;
        this.rowOffset = requireNonNull(rowOffset, "Row offset wasn't specified");
        this.rowCount = requireNonNull(rowCount, "Row count wasn't specified");
    }

    public int getGroupIndex()
    {
        return groupIndex;
    }

    public Long getRowOffset()
    {
        return rowOffset;
    }

    public Long getRowCount()
    {
        return rowCount;
    }

    @SuppressWarnings("unused")
    public void setGroupIndex(int groupIndex)
    {
        this.groupIndex = groupIndex;
    }

    @SuppressWarnings("unused")
    public void setRowOffset(Long rowOffset)
    {
        this.rowOffset = rowOffset;
    }

    @SuppressWarnings("unused")
    public void setRowCount(Long rowCount)
    {
        this.rowCount = rowCount;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return "{'groupIndex':" + groupIndex + ", 'rowOffset':" + rowOffset + ", 'rowCount':" + rowCount + "}";
    }

    /**
     * Fluent-styled builder to create an instance of {@link GroupSplit}
     */
    public static class Builder
    {
        private Builder()
        {
        }

        private Integer groupIndex;
        private Long rowOffset;
        private Long rowCount;

        public Builder groupIndex(int groupIndex)
        {
            this.groupIndex = groupIndex;
            return this;
        }

        public Builder rowOffset(long rowOffset)
        {
            this.rowOffset = rowOffset;
            return this;
        }

        public Builder rowCount(long rowCount)
        {
            this.rowCount = rowCount;
            return this;
        }

        /**
         * Creates an instance of {@link GroupSplit}. It checks to see if the minimum required parameters exists (not null)
         *
         * @return An instance of {@link GroupSplit}
         */
        public GroupSplit build()
        {
            requireNonNull(this.groupIndex, "Group index is not set");
            requireNonNull(this.rowOffset, "Row offset is not set");
            requireNonNull(this.rowCount, "Row count is not set");
            return new GroupSplit(this.groupIndex, this.rowOffset, this.rowCount);
        }
    }
}
