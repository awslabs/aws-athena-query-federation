/*-
 * #%L
 * athena-aws-cmdb
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
package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TableProviderFactoryTest
{
    private int expectedSchemas = 4;
    private int expectedTables = 11;

    @Mock
    private AmazonEC2 mockEc2;

    @Mock
    private AmazonElasticMapReduce mockEmr;

    @Mock
    private AmazonRDS mockRds;

    @Mock
    private AmazonS3 amazonS3;

    private TableProviderFactory factory = new TableProviderFactory(mockEc2, mockEmr, mockRds, amazonS3, com.google.common.collect.ImmutableMap.of());

    @Test
    public void getTableProviders()
    {
        int count = 0;
        for (Map.Entry<TableName, TableProvider> next : factory.getTableProviders().entrySet()) {
            assertEquals(next.getKey(), next.getValue().getTableName());
            assertEquals(next.getKey().getSchemaName(), next.getValue().getSchema());
            count++;
        }
        assertEquals(expectedTables, count);
    }

    @Test
    public void getSchemas()
    {
        int schemas = 0;
        int tables = 0;
        for (Map.Entry<String, List<TableName>> next : factory.getSchemas().entrySet()) {
            for (TableName nextTableName : next.getValue()) {
                assertEquals(next.getKey(), nextTableName.getSchemaName());
                tables++;
            }
            schemas++;
        }
        assertEquals(expectedSchemas, schemas);
        assertEquals(expectedTables, tables);
    }
}
