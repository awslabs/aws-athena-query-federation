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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.Database;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesRequest;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesResult;
import com.amazonaws.services.timestreamwrite.model.ListTablesResult;
import com.amazonaws.services.timestreamwrite.model.Table;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamMetadataHandlerTest.class);

    private final String defaultSchema = "default";
    private final FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private TimestreamMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    protected AWSSecretsManager mockSecretsManager;
    @Mock
    protected AmazonAthena mockAthena;
    @Mock
    protected AmazonTimestreamQuery mockTsQuery;
    @Mock
    protected AmazonTimestreamWrite mockTsMeta;
    @Mock
    protected AWSGlue mockGlue;

    @Before
    public void setUp()
            throws Exception
    {
        handler = new TimestreamMetadataHandler(mockTsQuery,
                mockTsMeta,
                mockGlue,
                new LocalKeyFactory(),
                mockSecretsManager,
                mockAthena,
                "spillBucket",
                "spillPrefix");

        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        logger.info("doListSchemaNames - enter");

        when(mockTsMeta.listDatabases(any(ListDatabasesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListDatabasesRequest request = invocation.getArgumentAt(0, ListDatabasesRequest.class);

            String newNextToken = null;
            List<Database> databases = new ArrayList<>();
            if (request.getNextToken() == null) {
                for (int i = 0; i < 10; i++) {
                    databases.add(new Database().withDatabaseName("database_" + i));
                }
                newNextToken = "1";
            }
            else if (request.getNextToken().equals("1")) {
                for (int i = 10; i < 100; i++) {
                    databases.add(new Database().withDatabaseName("database_" + i));
                }
                newNextToken = "2";
            }
            else if (request.getNextToken().equals("2")) {
                for (int i = 100; i < 1000; i++) {
                    databases.add(new Database().withDatabaseName("database_" + i));
                }
                newNextToken = null;
            }

            return new ListDatabasesResult().withDatabases(databases).withNextToken(newNextToken);
        });

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemaNames - {}", res.getSchemas());

        assertEquals(1000, res.getSchemas().size());
        verify(mockTsMeta, times(3)).listDatabases(any(ListDatabasesRequest.class));
        Iterator<String> schemaItr = res.getSchemas().iterator();
        for (int i = 0; i < 1000; i++) {
            assertEquals("database_" + i, schemaItr.next());
        }

        logger.info("doListSchemaNames - exit");
    }

    @Test
    public void doListTables()
            throws Exception
    {
        logger.info("doListTables - enter");

        when(mockTsMeta.listTables(any(com.amazonaws.services.timestreamwrite.model.ListTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    com.amazonaws.services.timestreamwrite.model.ListTablesRequest request =
                            invocation.getArgumentAt(0, com.amazonaws.services.timestreamwrite.model.ListTablesRequest.class);

                    String newNextToken = null;
                    List<Table> tables = new ArrayList<>();
                    if (request.getNextToken() == null) {
                        for (int i = 0; i < 10; i++) {
                            tables.add(new Table().withDatabaseName(request.getDatabaseName()).withTableName("table_" + i));
                        }
                        newNextToken = "1";
                    }
                    else if (request.getNextToken().equals("1")) {
                        for (int i = 10; i < 100; i++) {
                            tables.add(new Table().withDatabaseName(request.getDatabaseName()).withTableName("table_" + i));
                        }
                        newNextToken = "2";
                    }
                    else if (request.getNextToken().equals("2")) {
                        for (int i = 100; i < 1000; i++) {
                            tables.add(new Table().withDatabaseName(request.getDatabaseName()).withTableName("table_" + i));
                        }
                        newNextToken = null;
                    }

                    return new ListTablesResult().withTables(tables).withNextToken(newNextToken);
                });

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default", defaultSchema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertEquals(1000, res.getTables().size());
        verify(mockTsMeta, times(3))
                .listTables(any(com.amazonaws.services.timestreamwrite.model.ListTablesRequest.class));

        Iterator<TableName> schemaItr = res.getTables().iterator();
        for (int i = 0; i < 1000; i++) {
            TableName tableName = schemaItr.next();
            assertEquals(defaultSchema, tableName.getSchemaName());
            assertEquals("table_" + i, tableName.getTableName());
        }

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        logger.info("doGetTable - enter");

        when(mockGlue.getTable(any(com.amazonaws.services.glue.model.GetTableRequest.class)))
                .thenReturn(mock(GetTableResult.class));

        when(mockTsQuery.query(any(QueryRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            QueryRequest request = invocation.getArgumentAt(0, QueryRequest.class);
            assertEquals("DESCRIBE \"default\".\"table1\"", request.getQueryString());
            List<Row> rows = new ArrayList<>();

            //TODO: Add types here
            rows.add(new Row().withData(new Datum().withScalarValue("availability_zone"),
                    new Datum().withScalarValue("varchar"),
                    new Datum().withScalarValue("dimension")));
            rows.add(new Row().withData(new Datum().withScalarValue("measure_value"),
                    new Datum().withScalarValue("double"),
                    new Datum().withScalarValue("measure_value")));
            rows.add(new Row().withData(new Datum().withScalarValue("measure_name"),
                    new Datum().withScalarValue("varchar"),
                    new Datum().withScalarValue("measure_name")));
            rows.add(new Row().withData(new Datum().withScalarValue("time"),
                    new Datum().withScalarValue("timestamp"),
                    new Datum().withScalarValue("timestamp")));

            return new QueryResult().withRows(rows);
        });

        GetTableRequest req = new GetTableRequest(identity,
                "query-id",
                "default",
                new TableName(defaultSchema, "table1"));

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(4, res.getSchema().getFields().size());

        Field measureName = res.getSchema().findField("measure_name");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(measureName.getType()));

        Field measureValue = res.getSchema().findField("measure_value");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(measureValue.getType()));

        Field availabilityZone = res.getSchema().findField("availability_zone");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(availabilityZone.getType()));

        Field time = res.getSchema().findField("time");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(time.getType()));

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableGlue()
            throws Exception
    {
        logger.info("doGetTable - enter");

        when(mockGlue.getTable(any(com.amazonaws.services.glue.model.GetTableRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            com.amazonaws.services.glue.model.GetTableRequest request = invocation.getArgumentAt(0,
                    com.amazonaws.services.glue.model.GetTableRequest.class);

            List<Column> columns = new ArrayList<>();
            columns.add(new Column().withName("col1").withType("varchar"));
            columns.add(new Column().withName("col2").withType("double"));
            com.amazonaws.services.glue.model.Table table = new com.amazonaws.services.glue.model.Table();
            table.setName(request.getName());
            table.setDatabaseName(request.getDatabaseName());
            StorageDescriptor storageDescriptor = new StorageDescriptor();
            storageDescriptor.setColumns(columns);
            table.setStorageDescriptor(storageDescriptor);
            table.setViewOriginalText("view text");
            table.setParameters(Collections.singletonMap("timestream-metadata-flag", "timestream-metadata-flag"));

            return new GetTableResult().withTable(table);
        });

        GetTableRequest req = new GetTableRequest(identity,
                "query-id",
                "default",
                new TableName(defaultSchema, "table1"));

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(2, res.getSchema().getFields().size());

        Field measureName = res.getSchema().findField("col1");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(measureName.getType()));

        Field measureValue = res.getSchema().findField("col2");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(measureValue.getType()));

        assertEquals("view text", res.getSchema().getCustomMetadata().get(VIEW_METADATA_FIELD));

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTimeSeriesTableGlue()
            throws Exception
    {
        logger.info("doGetTimeSeriesTableGlue - enter");

        when(mockGlue.getTable(any(com.amazonaws.services.glue.model.GetTableRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            com.amazonaws.services.glue.model.GetTableRequest request = invocation.getArgumentAt(0,
                    com.amazonaws.services.glue.model.GetTableRequest.class);

            List<Column> columns = new ArrayList<>();
            columns.add(new Column().withName("az").withType("varchar"));
            columns.add(new Column().withName("hostname").withType("varchar"));
            columns.add(new Column().withName("region").withType("varchar"));
            columns.add(new Column().withName("cpu_utilization").withType("ARRAY<STRUCT<time: timestamp, measure_value\\:\\:double: double>>"));
            com.amazonaws.services.glue.model.Table table = new com.amazonaws.services.glue.model.Table();
            table.setName(request.getName());
            table.setDatabaseName(request.getDatabaseName());
            StorageDescriptor storageDescriptor = new StorageDescriptor();
            storageDescriptor.setColumns(columns);
            table.setStorageDescriptor(storageDescriptor);
            table.setViewOriginalText("SELECT az, hostname, region, cpu_utilization FROM TIMESERIES(metrics_table,'cpu_utilization')");
            table.setParameters(Collections.singletonMap("timestream-metadata-flag", "timestream-metadata-flag"));

            return new GetTableResult().withTable(table);
        });

        GetTableRequest req = new GetTableRequest(identity,
                "query-id",
                "default",
                new TableName(defaultSchema, "table1"));

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(4, res.getSchema().getFields().size());

        Field measureName = res.getSchema().findField("az");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(measureName.getType()));

        Field hostname = res.getSchema().findField("hostname");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(hostname.getType()));

        Field region = res.getSchema().findField("region");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(region.getType()));

        Field cpuUtilization = res.getSchema().findField("cpu_utilization");
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(cpuUtilization.getType()));

        Field timeseries = cpuUtilization.getChildren().get(0);
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(timeseries.getType()));

        Field time = timeseries.getChildren().get(0);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(time.getType()));

        Field value = timeseries.getChildren().get(1);
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(value.getType()));

        assertEquals("SELECT az, hostname, region, cpu_utilization FROM TIMESERIES(metrics_table,'cpu_utilization')",
                res.getSchema().getCustomMetadata().get(VIEW_METADATA_FIELD));

        logger.info("doGetTimeSeriesTableGlue - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Schema schema = SchemaBuilder.newBuilder().build();
        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "query-id",
                defaultSchema,
                new TableName("database1", "table1"),
                new Constraints(new HashMap<>()),
                schema,
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() == 1);
        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        logger.info("doGetSplits - enter");

        List<String> partitionCols = new ArrayList<>();

        Block partitions = BlockUtils.newBlock(allocator, "partition_id", Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "query-id",
                defaultSchema,
                new TableName("database1", "table1"),
                partitions,
                partitionCols,
                new Constraints(new HashMap<>()),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertTrue("Continuation criteria violated", response.getSplits().size() == 1);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);

        logger.info("doGetSplits - exit");
    }
}
