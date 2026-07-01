/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ImpalaUtilsTest
{
    @Test
    public void quoteIdentifier_whenNameIsSimple_returnsBacktickWrappedName()
    {
        assertEquals("`a`", ImpalaUtils.quoteIdentifier("a"));
    }

    @Test
    public void quoteIdentifier_whenNameContainsBacktick_doublesEmbeddedBackticks()
    {
        assertEquals("`a``b`", ImpalaUtils.quoteIdentifier("a`b"));
    }

    @Test
    public void quoteIdentifier_whenIdentifierIsEmpty_returnsQuotedEmptyIdentifier()
    {
        assertEquals("``", ImpalaUtils.quoteIdentifier(""));
    }

    @Test(expected = NullPointerException.class)
    public void quoteIdentifier_whenIdentifierIsNull_throwsNullPointerException()
    {
        ImpalaUtils.quoteIdentifier(null);
    }

    @Test
    public void qualifiedTableForMetadataSql_whenTableNameIsStandard_returnsQuotedQualifiedName()
    {
        TableName tableName = new TableName("testSchema", "testTable");
        assertEquals("`TESTSCHEMA`.`TESTTABLE`", ImpalaUtils.qualifiedTableForMetadataSql(tableName));
    }

    @Test
    public void qualifiedTableForMetadataSql_whenTableNameHasSpecialCharacters_keepsCharactersInsideQuotes()
    {
        TableName tableName = new TableName("demo_security", "test; INSERT INTO x VALUES (1);--");
        String qualified = ImpalaUtils.qualifiedTableForMetadataSql(tableName);
        assertEquals("`DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`", qualified);
        assertEquals("describe FORMATTED `DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`",
                ImpalaMetadataHandler.GET_METADATA_QUERY + qualified);
        assertFalse(qualified.contains("`DEMO_SECURITY`.TEST;"));
    }

    @Test(expected = NullPointerException.class)
    public void qualifiedTableForMetadataSql_whenTableNameIsNull_throwsNullPointerException()
    {
        ImpalaUtils.qualifiedTableForMetadataSql(null);
    }
}
