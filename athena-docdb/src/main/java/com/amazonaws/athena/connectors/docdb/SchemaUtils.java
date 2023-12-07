/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Collection of helpful utilities that handle DocumentDB schema inference, type, and naming conversion.
 * <p>
 * Inferred Schemas are formed by scanning N documents from the desired collection and then performing a union
 * of all fields in those Documents. The same union approach is applied to complex types (structs aka nested Documents).
 * If a type mistmatch is discovered, we assume the type is VARCHAR since most types can be coerced to VARCHAR. However,
 * this naive coercion does not work well if you then try to filter on the coerced field because whifen we push the filter
 * into DocDB it will almost certainly result in no matches.
 */
public class SchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);

    private SchemaUtils() {}

    /**
     * This method will produce an Apache Arrow Schema for the given TableName and DocumentDB connection
     * by scanning up to the requested number of rows and using basic schema inference to determine
     * data types.
     *
     * @param client The DocumentDB connection to use for the scan operation.
     * @param table The DocumentDB TableName for which to produce an Apache Arrow Schema.
     * @param numObjToSample The number of records to scan as part of producing the Schema.
     * @return An Apache Arrow Schema representing the schema of the HBase table.
     * @note The resulting schema is a union of the schema of every row that is scanned. Presently the code does not
     * attempt to resolve conflicts if unique field has different types across documents. It is recommend that you
     * use AWS Glue to define a schema for tables which may have such conflicts. In the future we may enhance this method
     * to use a reasonable default (like String) and coerce heterogeneous fields to avoid query failure but forcing
     * explicit handling by defining Schema in AWS Glue is likely a better approach.
     */
    public static Schema inferSchema(MongoDatabase db, TableName table, int numObjToSample)
    {
        int docCount = 0;
        int fieldCount = 0;
        try (MongoCursor<Document> docs = db.getCollection(table.getTableName()).find().batchSize(numObjToSample)
                .limit(numObjToSample).iterator()) {
            if (!docs.hasNext()) {
                return SchemaBuilder.newBuilder().build();
            }
            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

            while (docs.hasNext()) {
                docCount++;
                Document doc = docs.next();
                for (String key : doc.keySet()) {
                    fieldCount++;
                    Field newField = getArrowField(key, doc.get(key));
                    Types.MinorType newType = Types.getMinorTypeForArrowType(newField.getType());
                    Field curField = schemaBuilder.getField(key);
                    Types.MinorType curType = (curField != null) ? Types.getMinorTypeForArrowType(curField.getType()) : null;

                    if (curField == null) {
                        schemaBuilder.addField(newField);
                    }
                    else if (newType != curType) {
                        //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
                        logger.warn("inferSchema: Encountered a mixed-type field[{}] {} vs {}, defaulting to String.",
                                key, curType, newType);
                        schemaBuilder.addStringField(key);
                    }
                    else if (curType == Types.MinorType.LIST) {
                        schemaBuilder.addField(mergeListField(key, curField, newField));
                    }
                    else if (curType == Types.MinorType.STRUCT) {
                        schemaBuilder.addField(mergeStructField(key, curField, newField));
                    }
                }
            }

            Schema schema = schemaBuilder.build();
            if (schema.getFields().isEmpty()) {
                throw new RuntimeException("No columns found after scanning " + fieldCount + " values across " +
                        docCount + " documents. Please ensure the collection is not empty and contains at least 1 supported column type.");
            }
            return schema;
        }
        finally {
            logger.info("inferSchema: Evaluated {} field values across {} documents.", fieldCount, docCount);
        }
    }

    /**
     * Used to merge LIST Field into a single Field. If called with two identical LISTs the output is essentially
     * the same as either of the inputs.
     *
     * @param fieldName The name of the merged Field.
     * @param curParentField The current field to use as the base for the merge.
     * @param newParentField The new field to merge into the base.
     * @return The merged field.
     */
    private static Field mergeListField(String fieldName, Field curParentField, Field newParentField)
    {
        //Apache Arrow lists have a special child that holds the concrete type of the list.
        Types.MinorType newInnerType = Types.getMinorTypeForArrowType(curParentField.getChildren().get(0).getType());
        Types.MinorType curInnerType = Types.getMinorTypeForArrowType(newParentField.getChildren().get(0).getType());
        if (newInnerType == Types.MinorType.LIST && curInnerType == Types.MinorType.LIST) {
            return FieldBuilder.newBuilder(fieldName, Types.MinorType.LIST.getType())
                    .addField(mergeStructField("", curParentField.getChildren().get(0), newParentField.getChildren().get(0))).build();
        }
        else if (curInnerType != newInnerType) {
            //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
            logger.warn("mergeListField: Encountered a mixed-type list field[{}] {} vs {}, defaulting to String.",
                    fieldName, curInnerType, newInnerType);
            return FieldBuilder.newBuilder(fieldName, Types.MinorType.LIST.getType()).addStringField("").build();
        }

        return curParentField;
    }

    /**
     * Used to merge STRUCT Field into a single Field. If called with two identical STRUCTs the output is essentially
     * the same as either of the inputs.
     *
     * @param fieldName The name of the merged Field.
     * @param curParentField The current field to use as the base for the merge.
     * @param newParentField The new field to merge into the base.
     * @return The merged field.
     */
    private static Field mergeStructField(String fieldName, Field curParentField, Field newParentField)
    {
        FieldBuilder union = FieldBuilder.newBuilder(fieldName, Types.MinorType.STRUCT.getType());
        for (Field nextCur : curParentField.getChildren()) {
            union.addField(nextCur);
        }

        for (Field nextNew : newParentField.getChildren()) {
            Field curField = union.getChild(nextNew.getName());
            if (curField == null) {
                union.addField(nextNew);
                continue;
            }

            Types.MinorType newType = Types.getMinorTypeForArrowType(nextNew.getType());
            Types.MinorType curType = Types.getMinorTypeForArrowType(curField.getType());

            if (curType != newType) {
                //TODO: currently we resolve fields with mixed types by defaulting to VARCHAR. This is _not_ ideal
                //for various reasons but also because it will cause predicate odities if used in a filter.
                logger.warn("mergeStructField: Encountered a mixed-type field[{}] {} vs {}, defaulting to String.",
                        nextNew.getName(), newType, curType);

                union.addStringField(nextNew.getName());
            }
            else if (curType == Types.MinorType.LIST) {
                union.addField(mergeListField(nextNew.getName(), curField, nextNew));
            }
            else if (curType == Types.MinorType.STRUCT) {
                union.addField(mergeStructField(nextNew.getName(), curField, nextNew));
            }
        }

        return union.build();
    }

    /**
     * Infers the type of a single DocumentDB document field.
     *
     * @param key The key of the field we are attempting to infer.
     * @param value A value from the key whose type we are attempting to infer.
     * @return The Apache Arrow field definition of the inferred key/value.
     */
    private static Field getArrowField(String key, Object value)
    {
        if (value instanceof String) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof Integer) {
            return new Field(key, FieldType.nullable(Types.MinorType.INT.getType()), null);
        }
        else if (value instanceof Long) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        }
        else if (value instanceof Boolean) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIT.getType()), null);
        }
        else if (value instanceof Float) {
            return new Field(key, FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        }
        else if (value instanceof Double) {
            return new Field(key, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        }
        else if (value instanceof Date) {
            return new Field(key, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        }
        else if (value instanceof BsonTimestamp) {
            return new Field(key, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        }
        else if (value instanceof ObjectId) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof List) {
            Field child;
            if (((List) value).isEmpty()) {
                logger.warn("getArrowType: Encountered an empty List/Array for field[{}], defaulting to List<String> due to type erasure.", key);
                return FieldBuilder.newBuilder(key, Types.MinorType.LIST.getType()).addStringField("").build();
            }
            else {
                child = getArrowField("", ((List) value).get(0));
            }
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),
                    Collections.singletonList(child));
        }
        else if (value instanceof Document) {
            List<Field> children = new ArrayList<>();
            Document doc = (Document) value;
            for (String childKey : doc.keySet()) {
                Object childVal = doc.get(childKey);
                Field child = getArrowField(childKey, childVal);
                children.add(child);
            }
            return new Field(key, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }

        String className = (value == null || value.getClass() == null) ? "null" : value.getClass().getName();
        logger.warn("Unknown type[" + className + "] for field[" + key + "], defaulting to varchar.");
        return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
    }
}
