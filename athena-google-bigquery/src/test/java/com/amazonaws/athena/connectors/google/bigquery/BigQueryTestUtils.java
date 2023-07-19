/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryTestUtils
{
    //public static final FederatedIdentity FEDERATED_IDENTITY = new FederatedIdentity("id",  "account", "principal",null);
    public static final String BOOL_FIELD_NAME_1 = "bool1";
    public static final String INTEGER_FIELD_NAME_1 = "int1";
    public static final String STRING_FIELD_NAME_1 = "string1";
    public static final String FLOAT_FIELD_NAME_1 = "float1";
    public static final String STRUCT_FIELD = "StructField";
    public static final String LIST_FIELD = "ListField";
    public static final String LIST_OF_STRUCT_FIELD = "ListOfStructField";
    public static final String PROJECT_1_NAME = "testProject";
    private static final String STRUCT_OF_LIST_FIELD = "StructOfListField";

    private BigQueryTestUtils()
    {
    }

    //Returns a list of mocked Datasets.
    static List<Dataset> getDatasetList(String projectName, int numDatasets)
    {
        List<Dataset> datasetList = new ArrayList<>();
        for (int i = 0; i < numDatasets; i++) {
            Dataset dataset1 = mock(Dataset.class);
            when(dataset1.getDatasetId()).thenReturn(DatasetId.of(projectName, "dataset" + i));
            datasetList.add(dataset1);
        }
        return datasetList;
    }

    //Returns a list of mocked Tables
    static List<Table> getTableList(String projectName, String dataset, int numTables)
    {
        List<Table> tableList = new ArrayList<>();
        for (int i = 0; i < numTables; i++) {
            Table table = mock(Table.class);
            when(table.getTableId()).thenReturn(TableId.of(projectName, dataset, "table" + i));
            tableList.add(table);
        }
        return tableList;
    }

    //Returns the schema by returning a list of fields in Google BigQuery Format.
    static List<Field> getTestSchemaFields()
    {
        return Arrays.asList(Field.of(BOOL_FIELD_NAME_1, LegacySQLTypeName.BOOLEAN),
                Field.of(INTEGER_FIELD_NAME_1, LegacySQLTypeName.INTEGER),
                Field.of(STRING_FIELD_NAME_1, LegacySQLTypeName.STRING),
                Field.of(FLOAT_FIELD_NAME_1, LegacySQLTypeName.FLOAT)
        );
    }

    //Returns the schema by returning a list of fields in Google BigQuery Format.
    static Schema getTestSchemaComplexSchema()
    {
        return Schema.of(Arrays.asList(
                        Field.of(STRUCT_FIELD, LegacySQLTypeName.RECORD, Field.of("intField", LegacySQLTypeName.INTEGER)),
                        Field.newBuilder(LIST_OF_STRUCT_FIELD, LegacySQLTypeName.RECORD, Field.of("intField", LegacySQLTypeName.INTEGER)).setMode(Field.Mode.REPEATED).build(),
                        Field.newBuilder(LIST_FIELD, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build(),
                        Field.of(STRUCT_OF_LIST_FIELD, LegacySQLTypeName.RECORD, Field.newBuilder(LIST_FIELD, LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build())
                )
        );
    }

    static Schema getTestSchema()
    {
        return Schema.of(getTestSchemaFields());
    }

    //Gets the schema in Arrow Format.
    public static org.apache.arrow.vector.types.pojo.Schema getBlockTestSchema()
    {
        return SchemaBuilder.newBuilder()
                .addBitField(BOOL_FIELD_NAME_1)
                .addIntField(INTEGER_FIELD_NAME_1)
                .addStringField(STRING_FIELD_NAME_1)
                .addFloat8Field(FLOAT_FIELD_NAME_1)
                .build();
    }

    static Collection<org.apache.arrow.vector.types.pojo.Field> getTestSchemaFieldsArrow()
    {
        return Arrays.asList(
                new org.apache.arrow.vector.types.pojo.Field(BOOL_FIELD_NAME_1,
                        FieldType.nullable(ArrowType.Bool.INSTANCE), null),
                new org.apache.arrow.vector.types.pojo.Field(INTEGER_FIELD_NAME_1,
                        FieldType.nullable(new ArrowType.Int(32, true)), null),
                new org.apache.arrow.vector.types.pojo.Field(STRING_FIELD_NAME_1,
                        FieldType.nullable(new ArrowType.Utf8()), null),
                new org.apache.arrow.vector.types.pojo.Field(FLOAT_FIELD_NAME_1,
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
        );
    }

    static org.apache.arrow.vector.types.pojo.Schema makeSchema(Map<String, ValueSet> constraintMap)
    {
        SchemaBuilder builder = new SchemaBuilder();
        for (Map.Entry<String, ValueSet> field : constraintMap.entrySet()) {
            ArrowType.ArrowTypeID typeId = field.getValue().getType().getTypeID();
            switch (typeId) {
                case Int:
                    builder.addIntField(field.getKey());
                    break;
                case Bool:
                    builder.addBitField(field.getKey());
                    break;
                case Utf8:
                    builder.addStringField(field.getKey());
                    break;
                default:
                    throw new UnsupportedOperationException("Type Not Implemented: " + typeId.name());
            }
        }
        return builder.build();
    }
}
