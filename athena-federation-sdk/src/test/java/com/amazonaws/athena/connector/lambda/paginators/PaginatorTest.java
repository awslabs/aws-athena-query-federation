/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.paginators;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandlerTest;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests the pagination logic in the Paginator class.
 */
@RunWith(MockitoJUnitRunner.class)
public class PaginatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataHandlerTest.class);

    private final List<TableName> rawResults = new ImmutableList.Builder<TableName>()
            .add(new TableName("schema", "table5"))
            .add(new TableName("schema", "table3"))
            .add(new TableName("schema", "table8"))
            .add(new TableName("schema", "table1"))
            .add(new TableName("schema", "table6"))
            .add(new TableName("schema", "table4"))
            .add(new TableName("schema", "table7"))
            .add(new TableName("schema", "table2"))
            .build();

    private final int pageSize = 3;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() {
        logger.info("{}: enter", testName.getMethodName());
    }

    @After
    public void cleanUp()
    {
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void getFirstPaginatedTest()
    {
        List<TableName> expectedResults = new ImmutableList.Builder<TableName>()
                .add(new TableName("schema", "table1"))
                .add(new TableName("schema", "table2"))
                .add(new TableName("schema", "table3"))
                .build();
        String startName = "";
        Paginator<TableName> tablePaginator = new Paginator<>(rawResults, startName, pageSize, TableName::getTableName);
        PaginatedResponse<TableName> paginatedResponse = tablePaginator.getNextPage();

        logger.info("Paginated Response: {}", paginatedResponse);

        assertEquals("Actual results do not match the expected values.", expectedResults,
                paginatedResponse.getResults());
        assertEquals("Actual nextName does not match the expected value.", "table4",
                paginatedResponse.getNextName());
    }

    @Test
    public void getNextPaginatedTest()
    {
        List<TableName> expectedResults = new ImmutableList.Builder<TableName>()
                .add(new TableName("schema", "table4"))
                .add(new TableName("schema", "table5"))
                .add(new TableName("schema", "table6"))
                .build();
        String startName = "table4";
        Paginator<TableName> tablePaginator = new Paginator<>(rawResults, startName, pageSize, TableName::getTableName);
        PaginatedResponse<TableName> paginatedResponse = tablePaginator.getNextPage();

        logger.info("Paginated Response: {}", paginatedResponse);

        assertEquals("Actual results do not match the expected values.", expectedResults,
                paginatedResponse.getResults());
        assertEquals("Actual nextName does not match the expected value.", "table7",
                paginatedResponse.getNextName());
    }

    @Test
    public void getLastPaginatedTest()
    {
        List<TableName> expectedResults = new ImmutableList.Builder<TableName>()
                .add(new TableName("schema", "table7"))
                .add(new TableName("schema", "table8"))
                .build();
        String startName = "table7";
        Paginator<TableName> tablePaginator = new Paginator<>(rawResults, startName, pageSize, TableName::getTableName);
        PaginatedResponse<TableName> paginatedResponse = tablePaginator.getNextPage();

        logger.info("Paginated Response: {}", paginatedResponse);

        assertEquals("Actual results do not match the expected values.", expectedResults,
                paginatedResponse.getResults());
        assertEquals("Actual nextName does not match the expected value.", "",
                paginatedResponse.getNextName());
    }
}
