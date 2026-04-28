/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TeradataQueryStringBuilderTest
{
    private TeradataQueryStringBuilder queryBuilder;
    private FederationExpressionParser federationExpressionParser;

    @Before
    public void setup()
    {
        federationExpressionParser = Mockito.mock(FederationExpressionParser.class);
        queryBuilder = new TeradataQueryStringBuilder("\"", federationExpressionParser);
    }

    @Test
    public void testGetFromClauseWithSplitFullyQualified()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit("testCatalog", "testSchema", "testTable", split);
        assertEquals(" FROM \"testCatalog\".\"testSchema\".\"testTable\" ", result);
    }

    @Test
    public void testGetFromClauseWithSplitNoSchema()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit("testCatalog", null, "testTable", split);
        assertEquals(" FROM \"testCatalog\".\"testTable\" ", result);
    }

    @Test
    public void testGetFromClauseWithSplitNoCatalog()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit(null, "testSchema", "testTable", split);
        assertEquals(" FROM \"testSchema\".\"testTable\" ", result);
    }

    @Test
    public void testGetFromClauseWithSplitTableOnly()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit(null, null, "testTable", split);
        assertEquals(" FROM \"testTable\" ", result);
    }

    @Test
    public void testGetPartitionWhereClausesWithPartition()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn("p1");
        
        List<String> result = queryBuilder.getPartitionWhereClauses(split);
        
        assertEquals(1, result.size());
        assertEquals("partition = p1", result.get(0));
    }

    @Test
    public void testGetPartitionWhereClausesWithWildcard()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn("*");
        
        List<String> result = queryBuilder.getPartitionWhereClauses(split);
        
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testAppendLimitOffset()
    {
        Split split = Mockito.mock(Split.class);
        Constraints constraints = Mockito.mock(Constraints.class);
        
        String result = queryBuilder.appendLimitOffset(split, constraints);
        
        assertEquals("", result);
    }

    @Test
    public void testGetSqlDialect()
    {
        assertEquals(TeradataSqlDialect.class, queryBuilder.getSqlDialect().getClass());
    }

}
