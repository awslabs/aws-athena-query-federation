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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TeradataSqlDialectTest
{
    private TeradataSqlDialect dialect;

    @Before
    public void setup()
    {
        dialect = new TeradataSqlDialect();
    }

    @Test
    public void testDialectIsInstanceOfCalciteTeradataDialect()
    {
        assertTrue("Should extend Calcite's TeradataSqlDialect",
                dialect instanceof org.apache.calcite.sql.dialect.TeradataSqlDialect);
    }

    @Test
    public void testUnparseOffsetFetchWritesNothing()
    {
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        dialect.unparseOffsetFetch(writer, null, null);
        assertEquals("unparseOffsetFetch should produce no output", "", writer.toSqlString().getSql().trim());
    }

    @Test
    public void testUnparseOffsetFetchWithNonNullValuesWritesNothing()
    {
        // Even when offset/fetch nodes are provided, nothing should be written
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        SqlNode mockOffset = org.mockito.Mockito.mock(SqlNode.class);
        SqlNode mockFetch = org.mockito.Mockito.mock(SqlNode.class);

        dialect.unparseOffsetFetch(writer, mockOffset, mockFetch);
        assertEquals("unparseOffsetFetch should produce no output even with non-null args",
                "", writer.toSqlString().getSql().trim());
    }
}
