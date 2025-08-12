/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class ArrowTypeComparatorTest {

    private static final String stringABC = "abc";
    private static final String stringXYZ = "xyz";
    private static final BigDecimal decimal1 = new BigDecimal("123.45");
    private static final BigDecimal decimal2 = new BigDecimal("678.90");
    private static final byte[] binary1 = {0x01, 0x02};
    private static final byte[] binary2 = {0x01, 0x03};
    private static final LocalDateTime date1 = LocalDateTime.of(2023, 1, 1, 12, 0);
    private static final LocalDateTime date2 = LocalDateTime.of(2023, 1, 2, 12, 0);

    private Map<String, Object> struct1;
    private Map<String, Object> struct2;

    private static final ArrowType intType = new ArrowType.Int(32, true);
    private static final ArrowType tinyIntType = new ArrowType.Int(8, true);
    private static final ArrowType smallIntType = new ArrowType.Int(16, true);
    private static final ArrowType uint2Type = new ArrowType.Int(16, false);
    private static final ArrowType bigIntType = new ArrowType.Int(64, true);
    private static final ArrowType float8Type = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    private static final ArrowType float4Type = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    private static final ArrowType varcharType = new ArrowType.Utf8();
    private static final ArrowType varbinaryType = new ArrowType.Binary();
    private static final ArrowType decimalType = new ArrowType.Decimal(10, 2, 128);
    private static final ArrowType bitType = new ArrowType.Bool();
    private static final ArrowType dateMilliType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneOffset.UTC.getId());
    private static final ArrowType dateDayType = new ArrowType.Date(DateUnit.DAY);
    private static final ArrowType structType = new ArrowType.Struct();
    private static final ArrowType unsupportedType = new ArrowType.Interval(IntervalUnit.YEAR_MONTH);

    @Before
    public void setUp() {
        struct1 = new HashMap<>();
        struct1.put("key1", "value1");
        struct1.put("key2", 42);

        struct2 = new HashMap<>();
        struct2.put("key1", "value1");
        struct2.put("key2", 43); // Different value for "key2"
    }

    private void assertComparison(int expected, ArrowType arrowType, Object lhs, Object rhs) {
        assertEquals(expected, ArrowTypeComparator.compare(arrowType, lhs, rhs));
    }

    private void assertStructComparison(int expected, Map<String, Object> lhs, Map<String, Object> rhs) {
        assertEquals(expected, ArrowTypeComparator.compare(structType, lhs, rhs));
    }

    @Test
    public void testCompareIntegers() {
        assertComparison(-1, intType, 5, 10);
        assertComparison(1, intType, 10, 5);
        assertComparison(0, intType, 5, 5);
    }

    @Test
    public void testCompareTinyInt() {
        assertComparison(-1, tinyIntType, (byte) 1, (byte) 2);
        assertComparison(1, tinyIntType, (byte) 2, (byte) 1);
        assertComparison(0, tinyIntType, (byte) 1, (byte) 1);
    }

    @Test
    public void testCompareSmallInt() {
        assertComparison(-100, smallIntType, (short) 100, (short) 200);
        assertComparison(100, smallIntType, (short) 200, (short) 100);
        assertComparison(0, smallIntType, (short) 100, (short) 100);
    }

    @Test
    public void testCompareUint2() {
        assertComparison(-1, uint2Type, 'A', 'B');
        assertComparison(1, uint2Type, 'B', 'A');
        assertComparison(0, uint2Type, 'A', 'A');
    }

    @Test
    public void testCompareBigInt() {
        assertComparison(-1, bigIntType, 1000L, 2000L);
        assertComparison(1, bigIntType, 2000L, 1000L);
        assertComparison(0, bigIntType, 1000L, 1000L);
    }

    @Test
    public void testCompareFloat8() {
        assertComparison(-1, float8Type, 123.45, 678.90);
        assertComparison(1, float8Type, 678.90, 123.45);
        assertComparison(0, float8Type, 123.45, 123.45);
    }

    @Test
    public void testCompareFloat4() {
        assertComparison(-1, float4Type, 123.45f, 678.90f);
        assertComparison(1, float4Type, 678.90f, 123.45f);
        assertComparison(0, float4Type, 123.45f, 123.45f);
    }

    @Test
    public void testCompareVarchar() {
        assertComparison(-23, varcharType, stringABC, stringXYZ);
        assertComparison(23, varcharType, stringXYZ, stringABC);
        assertComparison(0, varcharType, stringABC, stringABC);
    }

    @Test
    public void testCompareVarbinary() {
        assertComparison(-1, varbinaryType, binary1, binary2);
        assertComparison(1, varbinaryType, binary2, binary1);
        assertComparison(0, varbinaryType, binary1, binary1);
    }

    @Test
    public void testCompareDecimal() {
        assertComparison(-1, decimalType, decimal1, decimal2);
        assertComparison(1, decimalType, decimal2, decimal1);
        assertComparison(0, decimalType, decimal1, decimal1);
    }

    @Test
    public void testCompareBit() {
        assertComparison(1, bitType, true, false);
        assertComparison(-1, bitType, false, true);
        assertComparison(0, bitType, true, true);
    }

    @Test
    public void testCompareDateMilli() {
        assertComparison(-1, dateMilliType, date1, date2);
        assertComparison(1, dateMilliType, date2, date1);
        assertComparison(0, dateMilliType, date1, date1);
    }

    @Test
    public void testCompareDateDay() {
        assertComparison(-1, dateDayType, 1, 2);
        assertComparison(1, dateDayType, 2, 1);
        assertComparison(0, dateDayType, 1, 1);
    }

    @Test
    public void testCompareStructEqual() {
        struct2.put("key2", 42);

        assertStructComparison(0, struct1, struct2);
    }

    @Test
    public void testCompareStructNotEqual() {
        assertStructComparison(1, struct1, struct2);
    }

    @Test
    public void testCompareStructWithNull() {
        // Arrange: Set one struct to null
        struct2 = null;

        // Act & Assert: Expect struct1 (non-null) to be greater than struct2 (null)
        assertStructComparison(-1, struct1, struct2);

        // Act & Assert: Reverse comparison
        assertStructComparison(1, struct2, struct1);
    }

    @Test
    public void testCompareStructBothNull() {
        // Arrange: Set both structs to null
        struct1 = null;
        struct2 = null;

        // Act & Assert: Expect equality
        assertStructComparison(0, struct1, struct2);
    }

    @Test
    public void testCompareUnsupportedStructType() {
        // Act & Assert: Expect AthenaConnectorException
        AthenaConnectorException exception = assertThrows(
                AthenaConnectorException.class,
                () -> ArrowTypeComparator.compare(unsupportedType, struct1, struct2)
        );
        assertEquals("Unknown type INTERVALYEAR object: class java.util.HashMap", exception.getMessage());
    }

    @Test
    public void testUnsupportedType() {
        AthenaConnectorException exception = assertThrows(
                AthenaConnectorException.class,
                () -> ArrowTypeComparator.compare(unsupportedType, "lhs", "rhs")
        );
        assertEquals("Unknown type INTERVALYEAR object: class java.lang.String", exception.getMessage());
    }

    @Test
    public void testCompareNulls() {
        assertComparison(0, intType, null, null);
        assertComparison(1, intType, null, 10);
        assertComparison(-1, intType, 10, null);
    }
}
