/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.splits;

import org.apache.commons.lang3.Validate;

import java.util.Objects;

/**
 * Split information. Example: split range, column name, type and expected splits.
 *
 * @param <T> type of the split. E.g. Integer.
 */
public class SplitInfo<T>
{
    private final SplitRange<T> splitRange;
    private final String columnName;
    private final int columnType;
    private final int numSplits;

    /**
     * @param splitRange split range of type T.
     * @param columnName database column name.
     * @param columnType database column type.
     * @param numSplits expected number of splits.
     */
    public SplitInfo(final SplitRange<T> splitRange, final String columnName, final int columnType, final int numSplits)
    {
        this.splitRange = Validate.notNull(splitRange, "splitRange must not be null");
        this.columnName = Validate.notBlank(columnName, "columnName must not be blank");
        this.columnType = columnType;
        this.numSplits = numSplits < 1 ? 1 : numSplits;
    }

    public SplitRange<T> getSplitRange()
    {
        return splitRange;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getColumnType()
    {
        return columnType;
    }

    public int getNumSplits()
    {
        return numSplits;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitInfo<?> splitInfo = (SplitInfo<?>) o;
        return getColumnType() == splitInfo.getColumnType() &&
                getNumSplits() == splitInfo.getNumSplits() &&
                Objects.equals(getSplitRange(), splitInfo.getSplitRange()) &&
                Objects.equals(getColumnName(), splitInfo.getColumnName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSplitRange(), getColumnName(), getColumnType(), getNumSplits());
    }

    @Override
    public String toString()
    {
        return "SplitInfo{" +
                "splitRange=" + splitRange +
                ", columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                ", numSplits=" + numSplits +
                '}';
    }
}
