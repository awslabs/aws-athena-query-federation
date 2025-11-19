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
package com.amazonaws.athena.connector.lambda.serde.v6;

import com.amazonaws.athena.connector.lambda.data.ColumnStatistic;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableStatisticsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.TableNameSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class GetTableStatisticsResponseSerDeV6
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String ROWS_COUNT_FIELD = "rowsCount";
    private static final String TABLE_SIZE_FIELD = "tableSize";
    private static final String COLUMNS_STATS_FIELD = "columnsStatistics";
    private static final String COLUMNS_NAME_FIELD = "columnName";

    private GetTableStatisticsResponseSerDeV6() {}

    public static final class Serializer extends TypedSerializer<FederationResponse> implements VersionedSerDe.Serializer<FederationResponse>
    {
        private final TableNameSerDe.Serializer tableNameSerializer;
        private final VersionedSerDe.Serializer<ColumnStatistic> columnStatisticSerializer;

        public Serializer(TableNameSerDe.Serializer tableNameSerializer, VersionedSerDe.Serializer<ColumnStatistic> columnStatisticSerializer)
        {
            super(FederationResponse.class, GetTableStatisticsResponse.class);
            this.columnStatisticSerializer = columnStatisticSerializer;
            this.tableNameSerializer = tableNameSerializer;
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetTableStatisticsResponse response = (GetTableStatisticsResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, response.getCatalogName());
            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(response.getTableName(), jgen, provider);

            writeOptionalDouble(jgen, ROWS_COUNT_FIELD, response.getRowCounts());
            writeOptionalDouble(jgen, TABLE_SIZE_FIELD, response.getTableSize());

            jgen.writeObjectFieldStart(COLUMNS_STATS_FIELD);
            for (Map.Entry<String, ColumnStatistic> entry : response.getColumnsStatistics().entrySet()) {
                jgen.writeFieldName(entry.getKey());
                this.columnStatisticSerializer.serialize(entry.getValue(), jgen, provider);
            }
            jgen.writeEndObject();
        }

        private void writeOptionalDouble(JsonGenerator jgen, String fieldName, Optional<Double> optional) throws IOException
        {
            if (optional.isPresent()) {
                jgen.writeNumberField(fieldName, optional.get());
            }
            else {
                jgen.writeNullField(fieldName);
            }
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse> implements VersionedSerDe.Deserializer<FederationResponse>
    {
        private final TableNameSerDe.Deserializer tableNameDeserializer;
        private final VersionedSerDe.Deserializer<ColumnStatistic> columnStatisticDeserializer;

        public Deserializer(TableNameSerDe.Deserializer tableNameDeserializer, VersionedSerDe.Deserializer<ColumnStatistic> columnStatisticDeserializer)
        {
            super(FederationResponse.class, GetTableStatisticsResponse.class);
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
            this.columnStatisticDeserializer = requireNonNull(columnStatisticDeserializer, "columnStatisticDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, TABLE_NAME_FIELD);
            TableName tableName = tableNameDeserializer.deserialize(jparser, ctxt);

            Optional<Double> rowCount = Optional.ofNullable(getNextDoubleField(jparser, ROWS_COUNT_FIELD));
            Optional<Double> tableSize = Optional.ofNullable(getNextDoubleField(jparser, TABLE_SIZE_FIELD));

            assertFieldName(jparser, COLUMNS_STATS_FIELD);
            jparser.nextToken();

            Map<String, ColumnStatistic> columnStats = new HashMap<>();
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                String columnName = jparser.getCurrentName();
                ColumnStatistic columnStatistic = this.columnStatisticDeserializer.deserialize(jparser, ctxt);
                columnStats.put(columnName, columnStatistic);
            }
            return new GetTableStatisticsResponse(catalogName, tableName, rowCount, tableSize, columnStats);
        }
    }
}
