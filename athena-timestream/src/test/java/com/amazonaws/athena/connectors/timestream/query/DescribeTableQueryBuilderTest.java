/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.query;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.STGroupFile;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DescribeTableQueryBuilderTest
{
    private static final Logger logger = LoggerFactory.getLogger(DescribeTableQueryBuilderTest.class);
    private static final String DATABASE_NAME = "myDatabase";
    private static final String TABLE_NAME = "myTable";

    private QueryFactory queryFactory = new QueryFactory();

    @Test
    public void createGroupFile_ValidClasspathTemplate_ReturnsSTGroupFile() throws Exception
    {
        Method method = QueryFactory.class.getDeclaredMethod("createGroupFile");
        method.setAccessible(true);

        STGroupFile result = (STGroupFile) method.invoke(queryFactory);

        assertNotNull("Expected non-null result from createGroupFile", result);
    }

    @Test
    public void createLocalGroupFile_LocalFileExists_ReturnsSTGroupFile() throws Exception
    {
        Method method = QueryFactory.class.getDeclaredMethod("createLocalGroupFile");
        method.setAccessible(true);

        STGroupFile result = (STGroupFile) method.invoke(queryFactory);

        assertNotNull("Expected non-null result from createLocalGroupFile", result);
    }

    @Test
    public void build_WithDatabaseAndTableName_ReturnsDescribeTableQueryWithQuotedIdentifiers()
    {
        logger.info("build_WithDatabaseAndTableName_ReturnsDescribeTableQueryWithQuotedIdentifiers: enter");

        String expected = "DESCRIBE \"myDatabase\".\"myTable\"";

        String actual = queryFactory.createDescribeTableQueryBuilder()
                .withDatabaseName(DATABASE_NAME)
                .withTablename(TABLE_NAME)
                .build();

        logger.info("build_WithDatabaseAndTableName_ReturnsDescribeTableQueryWithQuotedIdentifiers: actual[{}]", actual);
        assertEquals("Built DESCRIBE query should match expected", expected, actual);

        logger.info("build_WithDatabaseAndTableName_ReturnsDescribeTableQueryWithQuotedIdentifiers: exit");
    }

    @Test(expected = NullPointerException.class)
    public void build_WithNullDatabaseName_ThrowsNullPointerException() {
        queryFactory.createDescribeTableQueryBuilder()
                .withDatabaseName(null)
                .withTablename(TABLE_NAME)
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WithNullTableName_ThrowsNullPointerException() {
        queryFactory.createDescribeTableQueryBuilder()
                .withDatabaseName(DATABASE_NAME)
                .withTablename(null)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void build_WithEmptyDatabaseName_ThrowsIllegalArgumentException() {
        queryFactory.createDescribeTableQueryBuilder()
                .withDatabaseName("")
                .withTablename(TABLE_NAME)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void build_WithEmptyTableName_ThrowsIllegalArgumentException() {
        queryFactory.createDescribeTableQueryBuilder()
                .withDatabaseName(DATABASE_NAME)
                .withTablename("")
                .build();
    }
}
