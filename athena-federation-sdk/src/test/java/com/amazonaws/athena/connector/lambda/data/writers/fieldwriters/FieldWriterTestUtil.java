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
package com.amazonaws.athena.connector.lambda.data.writers.fieldwriters;

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;


import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public final class FieldWriterTestUtil
{

    public static void configureBigIntExtractor(BigIntExtractor extractor, long value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableBigIntHolder holder = invocation.getArgument(1, NullableBigIntHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableBigIntHolder.class));
    }

    public static void configureBitExtractor(BitExtractor extractor, int value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableBitHolder holder = invocation.getArgument(1, NullableBitHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableBitHolder.class));
    }

    public static void configureDateDayExtractor(DateDayExtractor extractor, int value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableDateDayHolder holder = invocation.getArgument(1, NullableDateDayHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableDateDayHolder.class));
    }

    public static void configureDateMilliExtractor(DateMilliExtractor extractor, long value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableDateMilliHolder holder = invocation.getArgument(1, NullableDateMilliHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableDateMilliHolder.class));
    }

    public static void configureDecimalExtractor(DecimalExtractor extractor, BigDecimal value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableDecimalHolder holder = invocation.getArgument(1, NullableDecimalHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableDecimalHolder.class));
    }

    public static void configureFloat4Extractor(Float4Extractor extractor, float value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableFloat4Holder holder = invocation.getArgument(1, NullableFloat4Holder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableFloat4Holder.class));
    }

    public static void configureFloat8Extractor(Float8Extractor extractor, double value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableFloat8Holder holder = invocation.getArgument(1, NullableFloat8Holder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableFloat8Holder.class));
    }

    public static void configureIntExtractor(IntExtractor extractor, int value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableIntHolder holder = invocation.getArgument(1, NullableIntHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableIntHolder.class));
    }

    public static void configureSmallIntExtractor(SmallIntExtractor extractor, short value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableSmallIntHolder holder = invocation.getArgument(1, NullableSmallIntHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableSmallIntHolder.class));
    }

    public static void configureTinyIntExtractor(TinyIntExtractor extractor, byte value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableTinyIntHolder holder = invocation.getArgument(1, NullableTinyIntHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableTinyIntHolder.class));
    }

    public static void configureVarBinaryExtractor(VarBinaryExtractor extractor, byte[] value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableVarBinaryHolder holder = invocation.getArgument(1, NullableVarBinaryHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableVarBinaryHolder.class));
    }

    public static void configureVarCharExtractor(VarCharExtractor extractor, String value, int isSet) throws Exception
    {
        doAnswer(invocation -> {
            NullableVarCharHolder holder = invocation.getArgument(1, NullableVarCharHolder.class);
            holder.isSet = isSet;
            holder.value = value;
            return null;
        }).when(extractor).extract(any(), any(NullableVarCharHolder.class));
    }

}
