package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.rds.AmazonRDS;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TableProviderFactoryTest
{
    private int expectedSchemas = 3;
    private int expectedTables = 9;

    @Mock
    private AmazonEC2 mockEc2;

    @Mock
    private AmazonElasticMapReduce mockEmr;

    @Mock
    private AmazonRDS mockRds;

    private TableProviderFactory factory = new TableProviderFactory(mockEc2, mockEmr, mockRds);

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