/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BigQueryPredicateUtilsTest
{
    @Test
    public void testBuildBigQueryWhereClause_EmptyPredicates()
    {
        Map<String, List<ColumnPredicate>> emptyPredicates = new HashMap<>();
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(emptyPredicates);
        assertEquals("", result);
    }

    @Test
    public void testBuildBigQueryWhereClause_NullPredicates()
    {
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(null);
        assertEquals("", result);
    }

    @Test
    public void testBuildBigQueryWhereClause_SinglePredicate()
    {
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        ColumnPredicate predicate = new ColumnPredicate("age", SubstraitOperator.EQUAL, 25, null);
        predicates.put("age", Arrays.asList(predicate));
        
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(predicates);
        assertEquals("age = 25", result);
    }

    @Test
    public void testBuildBigQueryWhereClause_StringPredicate()
    {
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        ColumnPredicate predicate = new ColumnPredicate("name", SubstraitOperator.EQUAL, "John", null);
        predicates.put("name", Arrays.asList(predicate));
        
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(predicates);
        assertEquals("name = 'John'", result);
    }

    @Test
    public void testBuildBigQueryWhereClause_MultiplePredicates()
    {
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        ColumnPredicate agePredicate = new ColumnPredicate("age", SubstraitOperator.GREATER_THAN, 18, null);
        ColumnPredicate namePredicate = new ColumnPredicate("name", SubstraitOperator.NOT_EQUAL, "Admin", null);
        
        predicates.put("age", Arrays.asList(agePredicate));
        predicates.put("name", Arrays.asList(namePredicate));
        
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(predicates);
        assertTrue(result.contains("age > 18"));
        assertTrue(result.contains("name != 'Admin'"));
        assertTrue(result.contains(" AND "));
    }

    @Test
    public void testBuildBigQueryWhereClause_NullPredicatesTest()
    {
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        ColumnPredicate isNullPredicate = new ColumnPredicate("description", SubstraitOperator.IS_NULL, null, null);
        ColumnPredicate isNotNullPredicate = new ColumnPredicate("email", SubstraitOperator.IS_NOT_NULL, null, null);
        
        predicates.put("description", Arrays.asList(isNullPredicate));
        predicates.put("email", Arrays.asList(isNotNullPredicate));
        
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(predicates);
        assertTrue(result.contains("description IS NULL"));
        assertTrue(result.contains("email IS NOT NULL"));
        assertTrue(result.contains(" AND "));
    }

    @Test
    public void testBuildBigQueryWhereClause_ComparisonOperators()
    {
        Map<String, List<ColumnPredicate>> predicates = new HashMap<>();
        predicates.put("score", Arrays.asList(
            new ColumnPredicate("score", SubstraitOperator.GREATER_THAN_OR_EQUAL_TO, 80, null),
            new ColumnPredicate("score", SubstraitOperator.LESS_THAN, 100, null)
        ));
        
        String result = BigQueryPredicateUtils.buildBigQueryWhereClause(predicates);
        assertTrue(result.contains("score >= 80"));
        assertTrue(result.contains("score < 100"));
        assertTrue(result.contains(" AND "));
    }

    @Test
    public void testBuildFilterPredicatesFromPlan_NullPlan()
    {
        Map<String, List<ColumnPredicate>> result = BigQueryPredicateUtils.buildFilterPredicatesFromPlan(null);
        assertTrue(result.isEmpty());
    }
}
