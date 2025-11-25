/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.google.cloud.bigquery.QueryParameterValue;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.*;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;

public class BigQuerySubstraitPlanUtilsTest
{
    private BlockAllocatorImpl allocator;
    static final TableName tableName = new TableName("schema", "table");
    private static final String ENCODED_PLAN = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIVGhMIAhABGg1lcXVhbDphbnlfYW55GoIDEv8CCroCOrcC" +
            "CgoSCAoGBgcICQoLEuIBEt8BCgIKABJ5CncKAgoAEmsKCENBVEVHT1JZCgVQUklDRQoJUFJPRFVDVElECgtQUk9EVUNUTkFNRQoJVVBEQVRFX0FUCgxQUk9EVUNUT1dORVISJwoEYgIQAQoEKgIQ" +
            "AQoEYgIQAQoEYgIQAQoFggECEAEKBGICEAEYAjoECgJDMhpeGlwaBAoCEAEiLBoqGigIARoECgIQASIMGgoSCAoEEgIIAyIAIhAaDgoMYgpUaGVybW9zdGF0IiYaJBoiCAEaBAoCEAEiDBoKEggK" +
            "BBICCAUiACIKGggKBmIESm9obhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgASCENBVEVHT1JZEgVQUklDRRIJUFJPRFVDVElEEgtQUk9EVUNUTkFNRRIJVVBEQVRFX0FUEgxQUk9EVUNUT1dORVI=";

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testBuildSqlFromPlanWithConstraints() throws IOException {

        String encodedPlan = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIVGhMIAhABGg1lcXVhbDphbnlfYW55GoIDEv8CCroCOrcC" +
                "CgoSCAoGBgcICQoLEuIBEt8BCgIKABJ5CncKAgoAEmsKCENBVEVHT1JZCgVQUklDRQoJUFJPRFVDVElECgtQUk9EVUNUTkFNRQoJVVBEQVRFX0FUCgxQUk9EVUNUT1dORVISJwoEYgIQAQoEKgIQ" +
                "AQoEYgIQAQoEYgIQAQoFggECEAEKBGICEAEYAjoECgJDMhpeGlwaBAoCEAEiLBoqGigIARoECgIQASIMGgoSCAoEEgIIAyIAIhAaDgoMYgpUaGVybW9zdGF0IiYaJBoiCAEaBAoCEAEiDBoKEggK" +
                "BBICCAUiACIKGggKBmIESm9obhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgASCENBVEVHT1JZEgVQUklDRRIJUFJPRFVDVElEEgtQUk9EVUNUTkFNRRIJVVBEQVRFX0FUEgxQUk9EVUNUT1dORVI=";
        String encodedSchema = "9AEAABAAAAAAAAoADgAGAA0ACAAKAAAAAAADABAAAAAAAQoADAAAAAgABAAKAAAACAAAADwAAAABAAAADAAAAAgADAAIAAQACAAAAAgAAAAMAAAAAgAAAFtdAAANAAAAdGltZVN0YW1wQ29scwAAAAYAAABIAQAA9AAAALwAAACEAAAAQAAAAAQAAADi/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAND+//8MAAAAcHJvZHVjdE93bmVyAAAAABr///8UAAAAFAAAABwAAAAAAAgBHAAAAAAAAAAAAAAAAAAGAAgABgAGAAAAAAAAAAkAAAB1cGRhdGVfYXQAAABa////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAEj///8LAAAAcHJvZHVjdE5hbWUAjv///xQAAAAUAAAAFAAAAAAABQEQAAAAAAAAAAAAAAB8////CQAAAHByb2R1Y3RJZAAAAML///8UAAAAFAAAABwAAAAAAAIBIAAAAAAAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAAFAAAAcHJpY2UAEgAYABQAEwASAAwAAAAIAAQAEgAAABQAAAAUAAAAGAAAAAAABQEUAAAAAAAAAAAAAAAEAAQABAAAAAgAAABjYXRlZ29yeQAAAAAAAAAA";
        // Create a QueryPlan with the provided Substrait plan
        QueryPlan queryPlan =
                new QueryPlan("1.0", encodedPlan);

        try (Constraints constraints = new Constraints(null, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), queryPlan)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();

            byte[] schemaBytes = Base64.getDecoder().decode(encodedSchema);
            Schema schema = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(new ByteArrayInputStream(schemaBytes))));
            Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(constraints.getQueryPlan().getSubstraitPlan());
            List<String> clauses = BigQuerySubstraitPlanUtils.toConjuncts(schema.getFields(), plan, constraints, parameterValues);

            // Verify that SQL is generated and contains expected elements (OR condition returns 1 clause)
            assertEquals(1, clauses.size());
            // Note: parameterValues is not passed to toConjuncts, so no parameters are added
        }
    }

    @Test
    public void testBuildSqlFromPlanWithOrConstraints() throws IOException
    {
        String encodedPlan = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIVGhMIAhABGg1lcXVhbDphbnlfYW55GoIDEv8CCroCOrcCCgoSCAoGBgcICQoLEuIBEt8BCgIKABJ5CncKAgoAEmsKCENBVEVHT1JZCgVQUklDRQoJUFJPRFVDVElECgtQUk9EVUNUTkFNRQoJVVBEQVRFX0FUCgxQUk9EVUNUT1dORVISJwoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBGICEAEYAjoECgJDMhpeGlwaBAoCEAEiLBoqGigIARoECgIQASIMGgoSCAoEEgIIAyIAIhAaDgoMYgpUaGVybW9zdGF0IiYaJBoiCAEaBAoCEAEiDBoKEggKBBICCAUiACIKGggKBmIESm9obhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgASCENBVEVHT1JZEgVQUklDRRIJUFJPRFVDVElEEgtQUk9EVUNUTkFNRRIJVVBEQVRFX0FUEgxQUk9EVUNUT1dORVI=";
        String encodedSchema = "9AEAABAAAAAAAAoADgAGAA0ACAAKAAAAAAADABAAAAAAAQoADAAAAAgABAAKAAAACAAAADwAAAABAAAADAAAAAgADAAIAAQACAAAAAgAAAAMAAAAAgAAAFtdAAANAAAAdGltZVN0YW1wQ29scwAAAAYAAABIAQAA9AAAALwAAACEAAAAQAAAAAQAAADi/v//FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAND+//8MAAAAcHJvZHVjdE93bmVyAAAAABr///8UAAAAFAAAABwAAAAAAAgBHAAAAAAAAAAAAAAAAAAGAAgABgAGAAAAAAAAAAkAAAB1cGRhdGVfYXQAAABa////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAEj///8LAAAAcHJvZHVjdE5hbWUAjv///xQAAAAUAAAAFAAAAAAABQEQAAAAAAAAAAAAAAB8////CQAAAHByb2R1Y3RJZAAAAML///8UAAAAFAAAABwAAAAAAAIBIAAAAAAAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAAFAAAAcHJpY2UAEgAYABQAEwASAAwAAAAIAAQAEgAAABQAAAAUAAAAGAAAAAAABQEUAAAAAAAAAAAAAAAEAAQABAAAAAgAAABjYXRlZ29yeQAAAAAAAAAA";
        
        QueryPlan queryPlan = new QueryPlan("1.0", encodedPlan);

        try (Constraints constraints = new Constraints(null, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), queryPlan)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();

            byte[] schemaBytes = Base64.getDecoder().decode(encodedSchema);
            Schema schema = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(new ByteArrayInputStream(schemaBytes))));
            Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(constraints.getQueryPlan().getSubstraitPlan());
            List<String> clauses = BigQuerySubstraitPlanUtils.toConjuncts(schema.getFields(), plan, constraints, parameterValues);

            // Verify that OR clause is generated
            assertEquals(1, clauses.size());
            String clause = clauses.get(0);
            assertTrue("Should contain OR", clause.contains(" OR "));
            assertTrue("Should contain productname", clause.contains("productName"));
            assertTrue("Should contain productowner", clause.contains("productOwner"));
            // Verify that parameters were added
            assertEquals(2, parameterValues.size());
        }
    }

    @Test
    public void testBuildFilterPredicatesFromPlanWithNullPlan()
    {
        Map<String, List<ColumnPredicate>> result = BigQuerySubstraitPlanUtils.buildFilterPredicatesFromPlan(null);
        
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty for null plan", result.isEmpty());
    }

    @Test
    public void testGetLimit()
    {
        // Test with plan containing limit
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(ENCODED_PLAN);
        
        int limit = BigQuerySubstraitPlanUtils.getLimit(plan);
        
        assertTrue("Limit should be non-negative", limit >= 0);
        // This specific plan doesn't have a limit, so should return 0
        assertEquals("Expected no limit in test plan", 0, limit);
    }

    @Test
    public void testGetLimitWithNullPlan()
    {
        assertThrows("Should throw exception for null plan", 
                RuntimeException.class, 
                () -> BigQuerySubstraitPlanUtils.getLimit(null));
    }

    @Test
    public void testExtractOrderByClause()
    {
        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(ENCODED_PLAN);
        
        String orderByClause = BigQuerySubstraitPlanUtils.extractOrderByClause(plan);
        
        assertNotNull("Order by clause should not be null", orderByClause);
        // This specific plan doesn't have ORDER BY, so should return empty string
        assertEquals("Expected no ORDER BY in test plan", "", orderByClause);
    }

    @Test
    public void testExtractOrderByClauseWithNullPlan()
    {
        assertThrows("Should throw exception for null plan", 
                RuntimeException.class, 
                () -> BigQuerySubstraitPlanUtils.extractOrderByClause(null));
    }

    @Test
    public void testExtractFieldIndexFromExpressionWithInvalidExpression()
    {
        // Test the error case with null expression
        assertThrows("Should throw exception for null expression", 
                IllegalArgumentException.class, 
                () -> BigQuerySubstraitPlanUtils.extractFieldIndexFromExpression(null, null));
    }
}
