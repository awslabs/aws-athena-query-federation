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

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);

    private SchemaUtils() {}

    /**
     * Used to infer the schema from a sampling of documents.
     *
     * @param client
     * @param table
     * @param numObjToSample The number of documents to sample when inferring the schema.
     * @return The inferred schema.
     */
    public static Schema inferSchema(MongoClient client, TableName table, int numObjToSample)
    {
        MongoDatabase db = client.getDatabase(table.getSchemaName());

        try (MongoCursor<Document> docs = db.getCollection(table.getTableName()).find().batchSize(numObjToSample).iterator()) {
            if (!docs.hasNext()) {
                return SchemaBuilder.newBuilder().build();
            }
            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

            Set<String> discoveredColumns = new HashSet<>();
            while (docs.hasNext()) {
                Document doc = docs.next();
                for (String key : doc.keySet()) {
                    if (!discoveredColumns.contains(key)) {
                        schemaBuilder.addField(getArrowField(key, doc.get(key)));
                        discoveredColumns.add(key);
                    }
                }
            }

            return schemaBuilder.build();
        }
    }

    public static Field getArrowField(String key, Object value)
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
            return new Field(key, FieldType.nullable(Types.MinorType.TIMESTAMPSEC.getType()), null);
        }
        else if (value instanceof ObjectId) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof List) {
            Field child;
            if (((List) value).isEmpty()) {
                try {
                    Object subVal = ((List) value).getClass()
                            .getTypeParameters()[0].getGenericDeclaration().newInstance();
                    child = getArrowField("", subVal);
                }
                catch (IllegalAccessException | InstantiationException ex) {
                    throw new RuntimeException(ex);
                }
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

        String className = value.getClass() == null ? "null" : value.getClass().getName();
        throw new RuntimeException("Unknown type[" + className + "] for field[" + key + "]");
    }
}
