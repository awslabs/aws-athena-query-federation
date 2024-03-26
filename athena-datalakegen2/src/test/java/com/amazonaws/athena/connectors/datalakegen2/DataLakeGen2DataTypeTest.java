/*-
 * #%L
 * athena-datalakegen2
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.datalakegen2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataLakeGen2DataTypeTest {

    @Test
    void isSupported() {
        assertTrue(DataLakeGen2DataType.isSupported("bit"));
        assertTrue(DataLakeGen2DataType.isSupported("TINYINT"));
        assertTrue(DataLakeGen2DataType.isSupported("NUMERIC"));
        assertTrue(DataLakeGen2DataType.isSupported("SMALLMONEY"));
        assertTrue(DataLakeGen2DataType.isSupported("DATE"));
        assertTrue(DataLakeGen2DataType.isSupported("DATETIME"));
        assertTrue(DataLakeGen2DataType.isSupported("DATETIME2"));
        assertTrue(DataLakeGen2DataType.isSupported("SMALLDATETIME"));
        assertTrue(DataLakeGen2DataType.isSupported("DATETIMEOFFSET"));

        assertFalse(DataLakeGen2DataType.isSupported("unknownType"));
    }

    @Test
    void fromType()
    {
        assertEquals(DataLakeGen2DataType.BIT.getArrowType(), DataLakeGen2DataType.fromType("BIT"));
        assertEquals(DataLakeGen2DataType.TINYINT.getArrowType(), DataLakeGen2DataType.fromType("TINYINT"));
        assertEquals(DataLakeGen2DataType.NUMERIC.getArrowType(), DataLakeGen2DataType.fromType("NUMERIC"));
        assertEquals(DataLakeGen2DataType.SMALLMONEY.getArrowType(), DataLakeGen2DataType.fromType("SMALLMONEY"));
        assertEquals(DataLakeGen2DataType.DATE.getArrowType(), DataLakeGen2DataType.fromType("DATE"));
        assertEquals(DataLakeGen2DataType.DATETIME.getArrowType(), DataLakeGen2DataType.fromType("DATETIME"));
        assertEquals(DataLakeGen2DataType.DATETIME2.getArrowType(), DataLakeGen2DataType.fromType("DATETIME2"));
        assertEquals(DataLakeGen2DataType.SMALLDATETIME.getArrowType(), DataLakeGen2DataType.fromType("SMALLDATETIME"));
        assertEquals(DataLakeGen2DataType.DATETIMEOFFSET.getArrowType(), DataLakeGen2DataType.fromType("DATETIMEOFFSET"));
    }
}
