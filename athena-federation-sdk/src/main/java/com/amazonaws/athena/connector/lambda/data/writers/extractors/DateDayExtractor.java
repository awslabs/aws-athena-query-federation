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
package com.amazonaws.athena.connector.lambda.data.writers.extractors;

import org.apache.arrow.vector.holders.NullableDateDayHolder;

/**
 * Used to extract a DateDay value from the context object. This interface enables the use of a pseudo-code generator
 * for RowWriter which reduces object and branching overhead when translating from your source system to Apache
 * Arrow.
 * <p>
 * For example of how to use this, see ExampleRecordHandler in athena-federation-sdk.
 */
public interface DateDayExtractor
        extends Extractor
{
    /**
     * Used to extract a value from the context.
     *
     * @param context This is the object you provided to GeneratorRowWriter and is frequently the handle to the source
     * system row/query from which you need to extract a value.
     * @param dst The 'Holder' that you should write your value to and optionally set the isSet flag to > 0 for non-null
     * or 0 for null.
     * @throws Exception internal exception
     */
    void extract(Object context, NullableDateDayHolder dst) throws Exception;
}
