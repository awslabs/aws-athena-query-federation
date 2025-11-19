/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.data.ColumnStatistic;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GetTableStatisticsResponse extends MetadataResponse
{
    private TableName tableName;
    private Map<String, ColumnStatistic> columnsStatistics;
    private Optional<Double> rowCounts;
    private Optional<Double> tableSize;

    public GetTableStatisticsResponse(@JsonProperty("catalogName") String catalogName,
                                      @JsonProperty("tableName") TableName tableName,
                                      @JsonProperty("rowsCount") Optional<Double> rowsCount,
                                      @JsonProperty("tableSize") Optional<Double> tableSize,
                                      @JsonProperty("columnsStatistics") Map<String, ColumnStatistic> columnsStatistics
                                      )
    {
        super(MetadataRequestType.GET_TABLE_STATISTICS, catalogName);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnsStatistics = requireNonNull(columnsStatistics, "ColumnsStatistics can't be null");
        this.rowCounts = rowsCount;
        this.tableSize = tableSize;
    }

    public TableName getTableName()
    {
        return tableName;
    }

    public Map<String, ColumnStatistic> getColumnsStatistics()
    {
        return columnsStatistics;
    }

    public Optional<Double> getRowCounts()
    {
        return rowCounts;
    }

    public Optional<Double> getTableSize()
    {
        return tableSize;
    }

    @Override
    public void close() throws Exception
    {
        //No Op
    }

    @Override
    public String toString()
    {
        return "GetTableStatisticsResponse{" +
                "tableName=" + tableName +
                ", columnsStatistics=" + columnsStatistics +
                ", rowCounts=" + rowCounts +
                ", tableSize=" + tableSize +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetTableStatisticsResponse response = (GetTableStatisticsResponse) o;
        return Objects.equals(tableName, response.tableName) && Objects.equals(columnsStatistics, response.columnsStatistics) && Objects.equals(rowCounts, response.rowCounts) && Objects.equals(tableSize, response.tableSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, columnsStatistics, rowCounts, tableSize);
    }
}
