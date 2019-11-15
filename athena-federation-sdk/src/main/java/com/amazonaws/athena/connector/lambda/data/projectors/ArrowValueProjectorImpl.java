package com.amazonaws.athena.connector.lambda.data.projectors;

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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Abstract class that shares common logic to create the {@link ArrowValueProjectorImpl.Projection Projection} instance.
 * {@link ArrowValueProjectorImpl.Projection}'s implementation is decided at runtime based on
 * input {@link Types.MinorType Arrow minor type}.
 */
public abstract class ArrowValueProjectorImpl
        implements ArrowValueProjector
{
    /**
     * Concrete implementation of ArrowValueProjectorImpl should invoke thie method to get the Projection instance.
     * @param minorType
     * @return Projection used by child class to do actual projection work.
     */
    protected Projection createValueProjection(Types.MinorType minorType)
    {
        switch (minorType) {
            case LIST:
            case STRUCT:
                return createComplexValueProjection(minorType);
            default:
                return createSimpleValueProjection(minorType);
        }
    }

    private Projection createSimpleValueProjection(Types.MinorType minorType)
    {
        switch (minorType) {
            case DATEMILLI:
                return (fieldReader) -> {
                    if (Objects.isNull(fieldReader.readLocalDateTime())) {
                        return null;
                    }
                    long millis = fieldReader.readLocalDateTime().toDateTime(org.joda.time.DateTimeZone.UTC).getMillis();
                    return Instant.ofEpochMilli(millis).atZone(BlockUtils.UTC_ZONE_ID).toLocalDateTime();
                };
            case TINYINT:
            case UINT1:
                return (fieldReader) -> fieldReader.readByte();
            case UINT2:
                return (fieldReader) -> fieldReader.readCharacter();
            case SMALLINT:
                return (fieldReader) -> fieldReader.readShort();
            case DATEDAY:
                return (fieldReader) -> {
                    Integer intVal = fieldReader.readInteger();
                    if (Objects.isNull(intVal)) {
                        return null;
                    }
                    return LocalDate.ofEpochDay(intVal);
                };
            case INT:
            case UINT4:
                return (fieldReader) -> fieldReader.readInteger();
            case UINT8:
            case BIGINT:
                return (fieldReader) -> fieldReader.readLong();
            case DECIMAL:
                return (fieldReader) -> fieldReader.readBigDecimal();
            case FLOAT4:
                return (fieldReader) -> fieldReader.readFloat();
            case FLOAT8:
                return (fieldReader) -> fieldReader.readDouble();
            case VARCHAR:
                return (fieldReader) -> {
                    Text text = fieldReader.readText();
                    if (Objects.isNull(text)) {
                        return null;
                    }
                    return text.toString();
                };
            case VARBINARY:
                return (fieldReader) -> fieldReader.readByteArray();
            case BIT:
                return (fieldReader) -> fieldReader.readBoolean();
            default:
                throw new IllegalArgumentException("Unsupported type " + minorType);
        }
    }

    private Projection createComplexValueProjection(Types.MinorType minorType)
    {
        switch (minorType) {
            case LIST:
                return (fieldReader) -> {
                    ListArrowValueProjector subListProjector = new ListArrowValueProjector(fieldReader);
                    return subListProjector.doProject();
                };
            case STRUCT:
                return (fieldReader) -> {
                    StructArrowValueProjector subStructProjector = new StructArrowValueProjector(fieldReader);
                    return subStructProjector.doProject();
                };
            default:
                throw new IllegalArgumentException("Unsupported type " + minorType);
        }
    }

    interface Projection
    {
        Object doProjection(FieldReader fieldReader);
    }
}
