/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.datasource.parquet.filter;

import com.amazonaws.athena.storage.common.FilterExpression;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParquetConstraintEvaluatorTest
{
    private static final List<FilterExpression> expressions = new ArrayList<>();

    @BeforeClass
    public static void setupExpressions()
    {
        expressions.add(new EqualsExpression(0, null, 1L));
        expressions.add(new GreaterThanExpression(1, null, 10));
        expressions.add(new GreaterThanOrEqualExpression(2, null, 10));
        expressions.add(new IsNotNullExpression(3, null));
        expressions.add(new IsNullExpression(4, null));
        expressions.add(new LessThanExpression(5, null, 10));
        expressions.add(new LessThanOrEqualExpression(6, null, 10));

    }

    @Test
    public void testParquetConstraintEvaluationAllTrue()
    {
        ParquetConstraintEvaluator evaluator = new ParquetConstraintEvaluator(expressions);
        assertTrue("Value at index 0 didn't match", evaluator.evaluate(0, 1L));
        assertTrue("Value at index 1 didn't match", evaluator.evaluate(1, 11));
        assertTrue("Value at index 2 didn't match", evaluator.evaluate(2, 10));
        assertTrue("Value at index 3 didn't match", evaluator.evaluate(3, 10));
        assertTrue("Value at index 4 didn't match", evaluator.evaluate(4, null));
        assertTrue("Value at index 5 didn't match", evaluator.evaluate(5, 9));
        assertTrue("Value at index 6 didn't match", evaluator.evaluate(6, 10));
    }

    @Test
    public void testParquetConstraintEvaluationAllFalse()
    {
        ParquetConstraintEvaluator evaluator = new ParquetConstraintEvaluator(expressions);
        assertFalse("Value at index 1 matches", evaluator.evaluate(1, 10));
        assertFalse("Value at index 2 matches", evaluator.evaluate(2, 9));
        assertFalse("Value at index 0 matches", evaluator.evaluate(0, 2L));
        assertFalse("Value at index 3 matches", evaluator.evaluate(3, null));
        assertFalse("Value at index 4 matches", evaluator.evaluate(4, 10));
        assertFalse("Value at index 5 matches", evaluator.evaluate(5, 12));
        assertFalse("Value at index 6 matches", evaluator.evaluate(6, 12));
    }
}
