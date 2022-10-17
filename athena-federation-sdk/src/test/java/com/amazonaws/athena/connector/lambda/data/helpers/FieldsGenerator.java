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
package com.amazonaws.athena.connector.lambda.data.helpers;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import net.jqwik.api.*;

import java.util.List;
import java.util.TimeZone;

public class FieldsGenerator {

    private int counter = 0; // use to guarentee field names are unique for structs
    private boolean DEFAULT_NULLABLE = true;
    private boolean allowComplexKeys = true; // whether key for maps can be complex values
    private int maxRecursionDepth;

    public FieldsGenerator(int maxRecursionDepth) {
        this.maxRecursionDepth = maxRecursionDepth;
    }

    public FieldsGenerator(int maxRecursionDepth, boolean allowComplexKeys) {
        this.maxRecursionDepth = maxRecursionDepth;
        this.allowComplexKeys = allowComplexKeys;
    }

    @Provide
    public Arbitrary<Field> field() {
        return field(0, null, DEFAULT_NULLABLE);
    }

    private Arbitrary<Field> field(int depth, String name, boolean nullable) {
        return
            fieldType(depth+1, nullable).flatMap(fieldType ->
                fieldChildren(fieldType, depth+1).flatMap(children -> {
                    String fieldName = name;
                    if (name == null) {
                        counter++;
                        fieldName = Arbitraries.strings().ofMinLength(1).sample() + counter;
                    }
                    return Arbitraries.just(new Field(fieldName, fieldType, children));
                })
            );
    }

    private Arbitrary<FieldType> intField(boolean nullable) {
        return Arbitraries.of(true, false).flatMap(signed ->
            Arbitraries.of(8, 16, 32, 64).map(bits ->
                new FieldType(nullable, new ArrowType.Int(bits, signed), null)
            )
        );
    }

    private Arbitrary<FieldType> dateField(boolean nullable) {
        return Arbitraries.of(DateUnit.DAY, DateUnit.MILLISECOND).map(unit ->
            new FieldType(nullable, new ArrowType.Date(unit), null)
        );
    }

    private Arbitrary<FieldType> floatingPointField(boolean nullable) {
        // FloatingPointPrecision.HALF not supported yet in BlockUtils yet
        return Arbitraries.of(FloatingPointPrecision.SINGLE, FloatingPointPrecision.DOUBLE).map(precision ->
            new FieldType(nullable, new ArrowType.FloatingPoint(precision), null)
        );
    }

    private Arbitrary<FieldType> timestampField(boolean nullable) {
        return Arbitraries.of(TimeZone.getAvailableIDs()).fixGenSize(5).flatMap(timezone ->
            Arbitraries.of(TimeUnit.SECOND, TimeUnit.MILLISECOND, TimeUnit.MICROSECOND, TimeUnit.NANOSECOND).map(unit ->
                new FieldType(nullable, new ArrowType.Timestamp(unit, timezone), null)
            )
        );
    }

    private Arbitrary<FieldType> decimalField(boolean nullable) {
        return Arbitraries.integers().between(1,20).flatMap(precision ->
            Arbitraries.integers().greaterOrEqual(0).lessOrEqual(precision-1).flatMap(scale ->
                Arbitraries.of(32, 64, 128).map(bitWidth ->
                    new FieldType(nullable, new ArrowType.Decimal(precision, scale, bitWidth), null)
                )
            )
        );
    }

    private Arbitrary<FieldType> primitiveFieldType(boolean nullable) {
        return Arbitraries.oneOf(
            Arbitraries.just(new FieldType(nullable, new ArrowType.Binary(), null))
            , Arbitraries.just(new FieldType(nullable, new ArrowType.Bool(), null))
            , dateField(nullable)
            , decimalField(nullable)
            , intField(nullable)
            , floatingPointField(nullable)
            // TODO: Known failure, Arrow has a bug with writing some types to Lists like ArrowType.Timestamp
            // Uncomment once Arrow has resolve
            //, timestampField(nullable)
            , Arbitraries.just(new FieldType(nullable, new ArrowType.Utf8(), null))
        );
    }

    private Arbitrary<FieldType> complexFieldType(boolean nullable) {
        return Arbitraries.of(
            new FieldType(nullable, new ArrowType.List(), null)
            , new FieldType(nullable, new ArrowType.Struct(), null)
            , new FieldType(nullable, new ArrowType.Map(true), null)
        );
    }

    private Arbitrary<FieldType> fieldType(int depth, boolean nullable) {
        if (depth >= maxRecursionDepth) {
            return primitiveFieldType(nullable);
        }

        return Arbitraries.oneOf(
            primitiveFieldType(nullable)
            , complexFieldType(nullable)
        );
    }

    private Arbitrary<List<Field>> fieldChildren(FieldType parentType, int depth) {
        if (parentType.getType().equals(ArrowType.List.INSTANCE)) {
            return field(depth, null, DEFAULT_NULLABLE).list().ofSize(1);
        }

        if (parentType.getType().equals(ArrowType.Struct.INSTANCE)) {
            return field(depth, null, DEFAULT_NULLABLE).list().uniqueElements().ofMinSize(1).ofMaxSize(3);
        }

        if (parentType.getType().getTypeID().equals(ArrowType.Map.TYPE_TYPE)) {
            Arbitrary<Field> keyField = null;
            if (allowComplexKeys) {
                keyField = field(depth, MapVector.KEY_NAME, false);
            }
            else {
                // If maps of keys cannot be complex values, pass in maxRecursionDepth
                // to guarantee field will not be complex
                keyField = field(maxRecursionDepth, MapVector.KEY_NAME, false);
            }
            Arbitrary<Field> valueField = field(depth, MapVector.VALUE_NAME, DEFAULT_NULLABLE);

            FieldType structType = new FieldType(false, ArrowType.Struct.INSTANCE, null);
            Arbitrary<List<Field>> structField = Combinators.combine(keyField, valueField).as((key, value) ->
                new Field(MapVector.DATA_VECTOR_NAME,
                    structType,
                    List.of(key, value)
            )).list().ofSize(1);
            return structField;
        }

        return Arbitraries.just((java.util.List<Field>) null);
    }
}
