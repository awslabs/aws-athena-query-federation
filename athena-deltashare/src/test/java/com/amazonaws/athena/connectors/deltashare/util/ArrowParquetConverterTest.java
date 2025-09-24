/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import org.junit.Test;

public class ArrowParquetConverterTest
{
    @Test(expected = NullPointerException.class)
    public void testWriteParquetRecordToBlockWithNullParameters()
    {
        ArrowParquetConverter.writeParquetRecordToBlock(null, 0, null, null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testWriteParquetRecordToBlockWithNegativeRowIndex()
    {
        ArrowParquetConverter.writeParquetRecordToBlock(null, -1, null, null);
    }
}
