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
package com.amazonaws.athena.connector.lambda.data.writers.fieldwriters;

import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.vector.FieldVector;

/**
 * Used create FieldWriters which are used to write a value and apply constraints for a particular column to the row currently being processed.
 * This interface enables the use of a pseudo-code generator for RowWriter which reduces object and branching
 * overhead when translating from your source system to Apache. An important usage of this interface is to
 * override or supplement the default FieldWriters supported by GeneratedRowWriter.
 * <p>
 * For example of how to use this, see ExampleRecordHandler in athena-federation-sdk.
 *
 * @see FieldWriter
 */
public interface FieldWriterFactory
{
    /**
     * Creates a new instance of this FieldWriter and configures is such that writing a value required minimal
     * branching or secondary operations (metadata lookups, etc..)
     *
     * @param extractor The Extractor that can be used to obtain the value for the required column from the context.
     * @param vector The ApacheArrow vector to write the value to.
     * @param constraint The Constraint to apply to the value when returning if the value was valid.
     * @return a FieldWriter that is configured to use the above inputs when called upon to write a row.
     */
    FieldWriter create(FieldVector vector, Extractor extractor, ConstraintProjector constraint);
}
