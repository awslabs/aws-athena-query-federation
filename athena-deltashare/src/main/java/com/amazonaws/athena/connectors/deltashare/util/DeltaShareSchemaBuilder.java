/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds Arrow schemas from Delta Share table metadata with proper type mapping.
 */
public class DeltaShareSchemaBuilder 
{
    private static final Logger logger = LoggerFactory.getLogger(DeltaShareSchemaBuilder.class);

    /**
     * Builds table schema from Delta Share metadata.
     */
    public static Schema buildTableSchema(JsonNode deltaSchema) 
    {
        logger.info("Building Arrow schema from Delta Share schema");
        
        List<Field> fields = new ArrayList<>();
        Set<String> partitionColumns = extractPartitionColumns(deltaSchema);
        
        JsonNode fieldsNode = deltaSchema.get("fields");
        if (fieldsNode != null && fieldsNode.isArray()) {
            for (JsonNode fieldNode : fieldsNode) {
                String fieldName = fieldNode.get("name").asText();
                String deltaType = fieldNode.get("type").asText();
                boolean nullable = fieldNode.has("nullable") ? fieldNode.get("nullable").asBoolean() : true;
                
                ArrowType arrowType = mapDeltaTypeToArrowType(deltaType);
                
                Field field = new Field(fieldName, new FieldType(nullable, arrowType, null), null);
                fields.add(field);
                
                logger.info("Mapped field: {} ({} -> {})", fieldName, deltaType, arrowType);
            }
        }
        
        Schema schema = new Schema(fields);
        logger.info("Schema built successfully with {} fields, {} partition columns", 
                   fields.size(), partitionColumns.size());
        
        return schema;
    }

    /**
     * Enhances partition schema with additional metadata columns.
     */
    public static void enhancePartitionSchema(SchemaBuilder builder, 
                                            GetTableLayoutRequest request) 
    {
        logger.info("Enhancing partition schema for table: {}", request.getTableName().getTableName());
        
        builder.addStringField("partition_id");
        builder.addStringField("file_path");
        builder.addBigIntField("file_size");
        builder.addIntField("row_group_index");
        
        logger.info("Partition schema enhancement complete");
    }

    /**
     * Maps Delta type to Arrow type.
     */
    public static ArrowType mapDeltaTypeToArrowType(String deltaType) 
    {
        switch (deltaType.toLowerCase()) {
            case "string":
                return ArrowType.Utf8.INSTANCE;
            case "integer":
            case "int":
                return new ArrowType.Int(32, true);
            case "long":
            case "bigint":
                return new ArrowType.Int(64, true);
            case "float":
                return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
            case "double":
                return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case "boolean":
                return ArrowType.Bool.INSTANCE;
            case "date":
                return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
            case "timestamp":
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
            case "binary":
                return ArrowType.Binary.INSTANCE;
            case "decimal":
                return new ArrowType.Decimal(18, 2, 128);
            default:
                logger.warn("Unknown Delta type '{}', mapping to string", deltaType);
                return ArrowType.Utf8.INSTANCE;
        }
    }

    /**
     * Extracts partition columns from Delta schema.
     */
    private static Set<String> extractPartitionColumns(JsonNode deltaSchema) 
    {
        Set<String> partitionColumns = new HashSet<>();
        
        JsonNode partitionColumnsNode = deltaSchema.get("partitionColumns");
        if (partitionColumnsNode != null && partitionColumnsNode.isArray()) {
            for (JsonNode partitionColumn : partitionColumnsNode) {
                partitionColumns.add(partitionColumn.asText());
            }
        }
        
        logger.info("Extracted {} partition columns: {}", partitionColumns.size(), partitionColumns);
        return partitionColumns;
    }

}
