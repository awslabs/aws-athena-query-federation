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
package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.UTC_ZONE_ID;

public class UnitTestBlockUtils
{

    /**
     * Used to get values (Integer, Double, String, etc) from the Arrow values in the fieldReader
     * @param fieldReader the field reader containing the arrow value
     * @return the value object in java type
     */
    public static Object getValue(FieldReader fieldReader, int pos)
    {
        fieldReader.setPosition(pos);

        Types.MinorType minorType = fieldReader.getMinorType();
        switch (minorType) {
            case DATEMILLI:
                if (Objects.isNull(fieldReader.readLocalDateTime())) {
                    return null;
                }
                long millis = fieldReader.readLocalDateTime().atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli();
                return Instant.ofEpochMilli(millis).atZone(BlockUtils.UTC_ZONE_ID).toLocalDateTime();
            case TINYINT:
            case UINT1:
                return fieldReader.readByte();
            case UINT2:
                return fieldReader.readCharacter();
            case SMALLINT:
                return fieldReader.readShort();
            case DATEDAY:
                Integer intVal = fieldReader.readInteger();
                if (Objects.isNull(intVal)) {
                    return null;
                }
                return LocalDate.ofEpochDay(intVal);
            case INT:
            case UINT4:
                return fieldReader.readInteger();
            case UINT8:
            case BIGINT:
                return fieldReader.readLong();
            case DECIMAL:
                return fieldReader.readBigDecimal();
            case FLOAT4:
                return fieldReader.readFloat();
            case FLOAT8:
                return fieldReader.readDouble();
            case VARCHAR:
                Text text = fieldReader.readText();
                if (Objects.isNull(text)) {
                    return null;
                }
                return text.toString();
            case VARBINARY:
                return fieldReader.readByteArray();
            case BIT:
                return fieldReader.readBoolean();
            case LIST:
                return readList(fieldReader);
            case STRUCT:
                return readStruct(fieldReader);
            default:
                throw new IllegalArgumentException("Unsupported type " + minorType);
        }
    }

    /**
     * Recursively read the values of a complex list
     * @param listReader
     * @return
     */
    private static List<Object> readList(FieldReader listReader)
    {
        if (!listReader.isSet()) {
            return null;
        }

        List<Object> list = new ArrayList<>();

        while (listReader.next()) {
            FieldReader subReader = listReader.reader();
            if (!subReader.isSet()) {
                list.add(null);
                continue;
            }
            list.add(getValue(subReader, subReader.getPosition()));
        }
        return list;
    }

    /**
     * Recursively reads the value of a complex struct object.
     * @param structReader
     * @return
     */
    private static Map<String, Object> readStruct(FieldReader structReader)
    {
        if (!structReader.isSet()) {
            return null;
        }

        List<Field> fields = structReader.getField().getChildren();

        Map<String, Object> nameToValues = new HashMap<>();
        for (Field child : fields) {
            FieldReader subReader = structReader.reader(child.getName());
            nameToValues.put(child.getName(), getValue(subReader, subReader.getPosition()));
        }

        return nameToValues;
    }

}
