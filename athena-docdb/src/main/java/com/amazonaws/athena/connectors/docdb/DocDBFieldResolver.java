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

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.mongodb.DBRef;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.Document;

import java.util.Map;
import java.util.function.Function;

/**
 * Used to resolve DocDB complex structures to Apache Arrow Types.
 *
 * @see com.amazonaws.athena.connector.lambda.data.FieldResolver
 */
public class DocDBFieldResolver
        implements FieldResolver
{
    protected static final FieldResolver DEFAULT_FIELD_RESOLVER = new DocDBFieldResolver();

    private DocDBFieldResolver() {}

    static final Map<String, Function<DBRef, String>> dbRefExtractor = Map.of(
            "_id", dbRef -> dbRef.getId().toString(),
            "_db", DBRef::getDatabaseName,
            "_ref", DBRef::getCollectionName
    );

    @Override
    public Object getFieldValue(Field field, Object value)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        if (minorType == Types.MinorType.LIST) {
            return TypeUtils.coerce(field, ((Document) value).get(field.getName()));
        }
        else if (value instanceof Document) {
            Object rawVal = ((Document) value).get(field.getName());
            return TypeUtils.coerce(field, rawVal);
        }
        else if (value instanceof DBRef) {
            return TypeUtils.coerce(field, dbRefExtractor.get(field.getName()).apply((DBRef) value));
        }
        throw new RuntimeException("Expected LIST or Document type but found " + minorType);
    }
}
