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

import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DateTimeConverterTest
{
    @Test
    public void testConvertInt96BytesToEpochMillisWithValidData()
    {
        byte[] validInt96 = new byte[12];
        
        validInt96[0] = 0x00; validInt96[1] = 0x00; validInt96[2] = 0x00; validInt96[3] = 0x00;
        validInt96[4] = 0x00; validInt96[5] = 0x00; validInt96[6] = 0x00; validInt96[7] = 0x00;
        validInt96[8] = 0x01; validInt96[9] = 0x00; validInt96[10] = 0x00; validInt96[11] = 0x00;
        
        long result = DateTimeConverter.convertInt96BytesToEpochMillis(validInt96);
        
        assertEquals(-210866716800000L, result);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConvertInt96BytesToEpochMillisWithInvalidLength()
    {
        byte[] invalidData = new byte[10];
        DateTimeConverter.convertInt96BytesToEpochMillis(invalidData);
    }
    
    @Test(expected = NullPointerException.class)
    public void testConvertInt96BytesToEpochMillisWithNullData()
    {
        DateTimeConverter.convertInt96BytesToEpochMillis(null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConvertInt96BytesToEpochMillisWithEmptyArray()
    {
        DateTimeConverter.convertInt96BytesToEpochMillis(new byte[0]);
    }
    
    @Test
    public void testConvertInt96BinaryToEpochMillis()
    {
        byte[] int96Data = new byte[12];
        int96Data[0] = 0x00; int96Data[1] = 0x00; int96Data[2] = 0x00; int96Data[3] = 0x00;
        int96Data[4] = 0x00; int96Data[5] = 0x00; int96Data[6] = 0x00; int96Data[7] = 0x00;
        int96Data[8] = 0x01; int96Data[9] = 0x00; int96Data[10] = 0x00; int96Data[11] = 0x00;
        
        Binary binary = Binary.fromConstantByteArray(int96Data);
        long result = DateTimeConverter.convertInt96BinaryToEpochMillis(binary);
        
        assertEquals(-210866716800000L, result);
    }
    
    @Test
    public void testEpochConversionLogic()
    {
        byte[] epochData = new byte[12];
        
        epochData[8] = (byte) 0x8C; epochData[9] = (byte) 0x25; 
        epochData[10] = (byte) 0x25; epochData[11] = 0x00;
        
        long result = DateTimeConverter.convertInt96BytesToEpochMillis(epochData);
        
        assertEquals(-530841600000L, result);
    }
    
    @Test
    public void testNanosecondCalculation()
    {
        byte[] nanosData = new byte[12];
        
        nanosData[0] = (byte) 0x80; nanosData[1] = (byte) 0x96; nanosData[2] = (byte) 0x98; nanosData[3] = 0x00;
        nanosData[4] = 0x00; nanosData[5] = 0x00; nanosData[6] = 0x00; nanosData[7] = 0x00;
        nanosData[8] = (byte) 0x8C; nanosData[9] = (byte) 0x25; 
        nanosData[10] = (byte) 0x25; nanosData[11] = 0x00;
        
        long result = DateTimeConverter.convertInt96BytesToEpochMillis(nanosData);
        
        assertEquals(-530841599990L, result);
    }
}
