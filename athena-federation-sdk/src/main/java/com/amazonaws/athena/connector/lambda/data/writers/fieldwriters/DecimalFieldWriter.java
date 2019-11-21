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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class DecimalFieldWriter
        implements FieldWriter
{
    private final NullableDecimalHolder holder = new NullableDecimalHolder();
    private final DecimalExtractor extractor;
    private final DecimalVector vector;
    private final ConstraintApplier constraint;

    public DecimalFieldWriter(DecimalExtractor extractor, DecimalVector vector, ConstraintProjector rawConstraint)
    {
        this.extractor = extractor;
        this.vector = vector;
        if (rawConstraint != null) {
            constraint = (NullableDecimalHolder value) -> rawConstraint.apply(value.isSet == 0 ? null : value.value);
        }
        else {
            constraint = (NullableDecimalHolder value) -> true;
        }
    }

    @Override
    public boolean write(Object context, int rowNum)
    {
        extractor.extract(context, rowNum, holder);
        if (holder.isSet > 0) {
            BigDecimal dVal = holder.value.setScale(vector.getScale(), RoundingMode.HALF_UP);
            vector.setSafe(rowNum, dVal);
        }
        else {
            vector.setNull(rowNum);
        }
        return constraint.apply(holder);
    }

    private interface ConstraintApplier
    {
        boolean apply(NullableDecimalHolder value);
    }
}
