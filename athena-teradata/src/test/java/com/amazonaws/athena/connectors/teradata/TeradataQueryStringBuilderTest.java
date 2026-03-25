/*-
 * #%L
 * athena-teradata
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connectors.teradata.TeradataConstants.TERADATA_QUOTE_CHARACTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TeradataQueryStringBuilderTest
{
    private TeradataQueryStringBuilder builder;

    @Before
    public void setup()
    {
        builder = new TeradataQueryStringBuilder(TERADATA_QUOTE_CHARACTER, new TeradataFederationExpressionParser(TERADATA_QUOTE_CHARACTER));
    }

    @Test
    public void getFromClauseWithSplit_withCatalogSchemaAndTable_includesQuotedQualifiers()
    {
        Split split = Mockito.mock(Split.class);
        String from = builder.getFromClauseWithSplit("myCatalog", "mySchema", "myTable", split);
        assertEquals(" FROM \"myCatalog\".\"mySchema\".\"myTable\" ", from);
    }

    @Test
    public void getFromClauseWithSplit_withCatalogOnly_omitsEmptySchema()
    {
        Split split = Mockito.mock(Split.class);
        String from = builder.getFromClauseWithSplit("cat", "", "tbl", split);
        assertEquals(" FROM \"cat\".\"tbl\" ", from);
    }

    @Test
    public void getFromClauseWithSplit_withSchemaOnly_omitsNullCatalog()
    {
        Split split = Mockito.mock(Split.class);
        String from = builder.getFromClauseWithSplit(null, "sch", "tbl", split);
        assertEquals(" FROM \"sch\".\"tbl\" ", from);
    }

    @Test
    public void getFromClauseWithSplit_withTableOnly_omitsNullCatalogAndSchema()
    {
        Split split = Mockito.mock(Split.class);
        String from = builder.getFromClauseWithSplit(null, null, "tbl", split);
        assertEquals(" FROM \"tbl\" ", from);
    }

    @Test
    public void getPartitionWhereClauses_withConcretePartitionValue_returnsEqualityClause()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn("p0");

        List<String> clauses = builder.getPartitionWhereClauses(split);

        assertEquals(
                Collections.singletonList(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME + " = p0"),
                clauses);
    }

    @Test
    public void getPartitionWhereClauses_withAllPartitionsWildcard_returnsEmptyList()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)).thenReturn(TeradataMetadataHandler.ALL_PARTITIONS);

        List<String> clauses = builder.getPartitionWhereClauses(split);

        assertTrue(clauses.isEmpty());
    }
}
