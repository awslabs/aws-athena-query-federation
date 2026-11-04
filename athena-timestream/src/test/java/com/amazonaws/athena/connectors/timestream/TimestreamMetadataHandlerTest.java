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
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.timestream.qpt.TimestreamQueryPassthrough;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamquery.model.ScalarType;
import software.amazon.awssdk.services.timestreamquery.model.Type;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Database;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamMetadataHandlerTest.class);

    private final String defaultSchema = "default";
    private final FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private TimestreamMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    protected SecretsManagerClient mockSecretsManager;
    @Mock
    protected AthenaClient mockAthena;
    @Mock
    protected TimestreamQueryClient mockTsQuery;
    @Mock
    protected TimestreamWriteClient mockTsMeta;
    @Mock
    protected GlueClient mockGlue;

    @Before
    public void setUp() {
        handler = new TimestreamMetadataHandler(mockTsQuery,
                mockTsMeta,
                mockGlue,
                new LocalKeyFactory(),
                mockSecretsManager,
                mockAthena,
                "spillBucket",
                "spillPrefix",
                com.google.common.collect.ImmutableMap.of());

        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        logger.info("doListSchemaNames - enter");

        when(mockTsMeta.listDatabases(nullable(ListDatabasesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListDatabasesRequest request = invocation.getArgument(0, ListDatabasesRequest.class);

            String newNextToken = null;
            List<Database> databases = new ArrayList<>();
            if (request.nextToken() == null) {
                for (int i = 0; i < 10; i++) {
                    databases.add(Database.builder().databaseName("database_" + i).build());
                }
                newNextToken = "1";
            }
            else if (request.nextToken().equals("1")) {
                for (int i = 10; i < 100; i++) {
                    databases.add(Database.builder().databaseName("database_" + i).build());
                }
                newNextToken = "2";
            }
            else if (request.nextToken().equals("2")) {
                for (int i = 100; i < 1000; i++) {
                    databases.add(Database.builder().databaseName("database_" + i).build());
                }
                newNextToken = null;
            }

            return ListDatabasesResponse.builder().databases(databases).nextToken(newNextToken).build();
        });

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemaNames - {}", res.getSchemas());

        assertEquals(1000, res.getSchemas().size());
        verify(mockTsMeta, times(3)).listDatabases(nullable(ListDatabasesRequest.class));
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

        when(mockTsMeta.listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest request =
                            invocation.getArgument(0, software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class);

                    String newNextToken = null;
                    List<Table> tables = new ArrayList<>();
                    if (request.nextToken() == null) {
                        for (int i = 0; i < 10; i++) {
                            tables.add(Table.builder().databaseName(request.databaseName()).tableName("table_" + i).build());
                        }
                        newNextToken = "1";
                    }
                    else if (request.nextToken().equals("1")) {
                        for (int i = 10; i < 100; i++) {
                            tables.add(Table.builder().databaseName(request.databaseName()).tableName("table_" + i).build());
                        }
                        newNextToken = "2";
                    }
                    else if (request.nextToken().equals("2")) {
                        for (int i = 100; i < 1000; i++) {
                            tables.add(Table.builder().databaseName(request.databaseName()).tableName("table_" + i).build());
                        }
                        newNextToken = null;
                    }

                    return software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse.builder().tables(tables).nextToken(newNextToken).build();
                });

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default", defaultSchema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertEquals(1000, res.getTables().size());
        verify(mockTsMeta, times(3))
                .listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class));

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

        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenReturn(software.amazon.awssdk.services.glue.model.GetTableResponse.builder().build());

        when(mockTsQuery.query(nullable(QueryRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            QueryRequest request = invocation.getArgument(0, QueryRequest.class);
            assertEquals("DESCRIBE \"default\".\"table1\"", request.queryString());
            List<Row> rows = new ArrayList<>();

            //TODO: Add types here
            rows.add(Row.builder().data(Datum.builder().scalarValue("availability_zone").build(),
                    Datum.builder().scalarValue("varchar").build(),
                    Datum.builder().scalarValue("dimension").build()).build());
            rows.add(Row.builder().data(Datum.builder().scalarValue("measure_value").build(),
                    Datum.builder().scalarValue("double").build(),
                    Datum.builder().scalarValue("measure_value").build()).build());
            rows.add(Row.builder().data(Datum.builder().scalarValue("measure_name").build(),
                    Datum.builder().scalarValue("varchar").build(),
                    Datum.builder().scalarValue("measure_name").build()).build());
            rows.add(Row.builder().data(Datum.builder().scalarValue("time").build(),
                    Datum.builder().scalarValue("timestamp").build(),
                    Datum.builder().scalarValue("timestamp").build()).build());
            rows.add(Row.builder().data(Datum.builder().scalarValue("sample_int").build(),
                    Datum.builder().scalarValue("int").build(),
                    Datum.builder().scalarValue("measure").build()).build());
            rows.add(Row.builder().data(Datum.builder().scalarValue("sample_date").build(),
                    Datum.builder().scalarValue("date").build(),
                    Datum.builder().scalarValue("dimension").build()).build());

            return QueryResponse.builder().rows(rows).build();
        });

        GetTableRequest req = new GetTableRequest(identity,
                "query-id",
                "default",
                new TableName(defaultSchema, "table1"), Collections.emptyMap());

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(6, res.getSchema().getFields().size());

        Field measureName = res.getSchema().findField("measure_name");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(measureName.getType()));

        Field measureValue = res.getSchema().findField("measure_value");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(measureValue.getType()));

        Field availabilityZone = res.getSchema().findField("availability_zone");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(availabilityZone.getType()));

        Field time = res.getSchema().findField("time");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(time.getType()));

        Field sampleInt = res.getSchema().findField("sample_int");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(sampleInt.getType()));

        Field sampleDate = res.getSchema().findField("sample_date");
        assertEquals(Types.MinorType.DATEDAY, Types.getMinorTypeForArrowType(sampleDate.getType()));

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableGlue()
            throws Exception
    {
        logger.info("doGetTable - enter");

        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            software.amazon.awssdk.services.glue.model.GetTableRequest request = invocation.getArgument(0,
            software.amazon.awssdk.services.glue.model.GetTableRequest.class);

            List<Column> columns = new ArrayList<>();
            columns.add(Column.builder().name("col1").type("varchar").build());
            columns.add(Column.builder().name("col2").type("double").build());
            StorageDescriptor storageDescriptor = StorageDescriptor.builder()
                    .columns(columns)
                    .build();
            software.amazon.awssdk.services.glue.model.Table table = software.amazon.awssdk.services.glue.model.Table.builder()
                    .name(request.name())
                    .databaseName(request.databaseName())
                    .storageDescriptor(storageDescriptor)
                    .viewOriginalText("view text")
                    .parameters(Collections.singletonMap("timestream-metadata-flag", "timestream-metadata-flag"))
                    .build();

            return software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        });

        GetTableRequest req = new GetTableRequest(identity,
                "query-id",
                "default",
                new TableName(defaultSchema, "table1"), Collections.emptyMap());

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

        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            software.amazon.awssdk.services.glue.model.GetTableRequest request = invocation.getArgument(0,
            software.amazon.awssdk.services.glue.model.GetTableRequest.class);

            List<Column> columns = new ArrayList<>();
            columns.add(Column.builder().name("az").type("varchar").build());
            columns.add(Column.builder().name("hostname").type("varchar").build());
            columns.add(Column.builder().name("region").type("varchar").build());
            columns.add(Column.builder().name("cpu_utilization").type("ARRAY<STRUCT<time: timestamp, measure_value\\:\\:double: double>>").build());
            StorageDescriptor storageDescriptor = StorageDescriptor.builder().columns(columns).build();
            software.amazon.awssdk.services.glue.model.Table table = software.amazon.awssdk.services.glue.model.Table.builder()
                    .name(request.name())
                    .databaseName(request.databaseName())
                    .storageDescriptor(storageDescriptor)
                    .viewOriginalText("SELECT az, hostname, region, cpu_utilization FROM TIMESERIES(metrics_table,'cpu_utilization')")
                    .parameters(Collections.singletonMap("timestream-metadata-flag", "timestream-metadata-flag"))
                    .build();

            return software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        });

        GetTableRequest req = new GetTableRequest(identity,
                "query-id",
                "default",
                new TableName(defaultSchema, "table1"), Collections.emptyMap());

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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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

    @Test
    public void doGetQueryPassthroughSchema_scalarColumns_returnsSchema()
            throws Exception
    {
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put(SCHEMA_FUNCTION_NAME, TimestreamQueryPassthrough.SCHEMA_NAME + "." + TimestreamQueryPassthrough.NAME);
        queryPassthroughArgs.put(TimestreamQueryPassthrough.QUERY,
                "SELECT id, measure_value, time FROM \"db\".\"table\"");

        GetTableRequest request = new GetTableRequest(identity, "query-id", "default",
                new TableName("system", "query"), queryPassthroughArgs);

        List<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(ColumnInfo.builder().name("id")
                .type(Type.builder().scalarType(ScalarType.VARCHAR).build()).build());
        columnInfos.add(ColumnInfo.builder().name("measure_value")
                .type(Type.builder().scalarType(ScalarType.DOUBLE).build()).build());
        columnInfos.add(ColumnInfo.builder().name("time")
                .type(Type.builder().scalarType(ScalarType.TIMESTAMP).build()).build());
        columnInfos.add(ColumnInfo.builder().name("day")
                .type(Type.builder().scalarType(ScalarType.DATE).build()).build());
        columnInfos.add(ColumnInfo.builder().name("cnt")
                .type(Type.builder().scalarType(ScalarType.INTEGER).build()).build());
        columnInfos.add(ColumnInfo.builder().name("clock")
                .type(Type.builder().scalarType(ScalarType.TIME).build()).build());
        columnInfos.add(ColumnInfo.builder().name("iv_day")
                .type(Type.builder().scalarType(ScalarType.INTERVAL_DAY_TO_SECOND).build()).build());
        columnInfos.add(ColumnInfo.builder().name("iv_month")
                .type(Type.builder().scalarType(ScalarType.INTERVAL_YEAR_TO_MONTH).build()).build());
        columnInfos.add(ColumnInfo.builder().name("unknown_col")
                .type(Type.builder().scalarType(ScalarType.UNKNOWN).build()).build());

        QueryResponse queryResponse = QueryResponse.builder()
                .columnInfo(columnInfos)
                .rows(Collections.emptyList())
                .build();
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenReturn(queryResponse);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, request);

        assertEquals(9, res.getSchema().getFields().size());
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(res.getSchema().findField("id").getType()));
        assertEquals(Types.MinorType.FLOAT8,
                Types.getMinorTypeForArrowType(res.getSchema().findField("measure_value").getType()));
        assertEquals(Types.MinorType.DATEMILLI,
                Types.getMinorTypeForArrowType(res.getSchema().findField("time").getType()));
        assertEquals(Types.MinorType.DATEDAY,
                Types.getMinorTypeForArrowType(res.getSchema().findField("day").getType()));
        assertEquals(Types.MinorType.INT,
                Types.getMinorTypeForArrowType(res.getSchema().findField("cnt").getType()));
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(res.getSchema().findField("clock").getType()));
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(res.getSchema().findField("iv_day").getType()));
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(res.getSchema().findField("iv_month").getType()));
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(res.getSchema().findField("unknown_col").getType()));
    }

    @Test
    public void doGetQueryPassthroughSchema_timeseriesColumn_returnsSchema()
            throws Exception
    {
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put(SCHEMA_FUNCTION_NAME, TimestreamQueryPassthrough.SCHEMA_NAME + "." + TimestreamQueryPassthrough.NAME);
        
        queryPassthroughArgs.put(TimestreamQueryPassthrough.QUERY,
                "SELECT id, my_time_series FROM \"db\".\"table\"");

        GetTableRequest request = new GetTableRequest(identity, "query-id", "default",
                new TableName("system", "query"), queryPassthroughArgs);

        ColumnInfo measureValueInfo = ColumnInfo.builder()
                .name("cpu_util")
                .type(Type.builder().scalarType(ScalarType.DOUBLE).build())
                .build();
        Type timeseriesType = Type.builder().timeSeriesMeasureValueColumnInfo(measureValueInfo).build();
        ColumnInfo timeseriesColumn = ColumnInfo.builder().name("my_time_series").type(timeseriesType).build();
        QueryResponse queryResponse = QueryResponse.builder()
                .columnInfo(Collections.singletonList(timeseriesColumn))
                .rows(Collections.emptyList())
                .build();

        when(mockTsQuery.query(nullable(QueryRequest.class))).thenReturn(queryResponse);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, request);

        Field ts = res.getSchema().findField("my_time_series");
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(ts.getType()));
        Field structField = ts.getChildren().get(0);
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(structField.getType()));
        assertEquals(Types.MinorType.DATEMILLI,
                Types.getMinorTypeForArrowType(structField.getChildren().get(0).getType()));
        assertEquals("cpu_util", structField.getChildren().get(1).getName());
        assertEquals(Types.MinorType.FLOAT8,
                Types.getMinorTypeForArrowType(structField.getChildren().get(1).getType()));
    }

    @Test
    public void doGetQueryPassthroughSchema_nullColumnName_defaultsToItem()
            throws Exception
    {
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put(SCHEMA_FUNCTION_NAME, TimestreamQueryPassthrough.SCHEMA_NAME + "." + TimestreamQueryPassthrough.NAME);
        queryPassthroughArgs.put(TimestreamQueryPassthrough.QUERY, "SELECT col FROM \"db\".\"table\"");

        GetTableRequest request = new GetTableRequest(identity, "query-id", "default",
                new TableName("system", "query"), queryPassthroughArgs);

        ColumnInfo unnamedColumn = ColumnInfo.builder()
                .type(Type.builder().scalarType(ScalarType.VARCHAR).build())
                .build();
        QueryResponse queryResponse = QueryResponse.builder()
                .columnInfo(Collections.singletonList(unnamedColumn))
                .rows(Collections.emptyList())
                .build();
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenReturn(queryResponse);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, request);
        assertEquals("item", res.getSchema().getFields().get(0).getName());
    }

    @Test
    public void doGetQueryPassthroughSchema_arrayAndRowColumns_returnsSchema()
            throws Exception
    {
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put(SCHEMA_FUNCTION_NAME, TimestreamQueryPassthrough.SCHEMA_NAME + "." + TimestreamQueryPassthrough.NAME);
        queryPassthroughArgs.put(TimestreamQueryPassthrough.QUERY, "SELECT arr, r FROM \"db\".\"table\"");

        GetTableRequest request = new GetTableRequest(identity, "query-id", "default",
                new TableName("system", "query"), queryPassthroughArgs);

        Type arrayType = Type.builder()
                .arrayColumnInfo(ColumnInfo.builder()
                        .type(Type.builder().scalarType(ScalarType.INTEGER).build())
                        .build())
                .build();
        Type rowType = Type.builder()
                .rowColumnInfo(
                        ColumnInfo.builder().name("x").type(Type.builder().scalarType(ScalarType.VARCHAR).build()).build(),
                        ColumnInfo.builder().name("y").type(Type.builder().scalarType(ScalarType.DOUBLE).build()).build(),
                        ColumnInfo.builder().type(Type.builder().scalarType(ScalarType.INTEGER).build()).build())
                .build();

        List<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(ColumnInfo.builder().name("arr").type(arrayType).build());
        columnInfos.add(ColumnInfo.builder().name("r").type(rowType).build());

        QueryResponse queryResponse = QueryResponse.builder()
                .columnInfo(columnInfos)
                .rows(Collections.emptyList())
                .build();
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenReturn(queryResponse);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, request);

        Field arr = res.getSchema().findField("arr");
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(arr.getType()));
        assertEquals("item", arr.getChildren().get(0).getName());
        assertEquals(Types.MinorType.INT,
                Types.getMinorTypeForArrowType(arr.getChildren().get(0).getType()));

        Field rowField = res.getSchema().findField("r");
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(rowField.getType()));
        assertEquals(3, rowField.getChildren().size());
        assertEquals("x", rowField.getChildren().get(0).getName());
        assertEquals(Types.MinorType.VARCHAR,
                Types.getMinorTypeForArrowType(rowField.getChildren().get(0).getType()));
        assertEquals("y", rowField.getChildren().get(1).getName());
        assertEquals(Types.MinorType.FLOAT8,
                Types.getMinorTypeForArrowType(rowField.getChildren().get(1).getType()));
        assertEquals("field_2", rowField.getChildren().get(2).getName());
        assertEquals(Types.MinorType.INT,
                Types.getMinorTypeForArrowType(rowField.getChildren().get(2).getType()));
    }

    @Test
    public void doGetQueryPassthroughSchema_missingType_throwsException()
            throws Exception
    {
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put(SCHEMA_FUNCTION_NAME, TimestreamQueryPassthrough.SCHEMA_NAME + "." + TimestreamQueryPassthrough.NAME);
        queryPassthroughArgs.put(TimestreamQueryPassthrough.QUERY, "SELECT missing_type FROM \"db\".\"table\"");

        GetTableRequest request = new GetTableRequest(identity, "query-id", "default",
                new TableName("system", "query"), queryPassthroughArgs);

        QueryResponse queryResponse = QueryResponse.builder()
                .columnInfo(Collections.singletonList(ColumnInfo.builder().name("missing_type").build()))
                .rows(Collections.emptyList())
                .build();
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenReturn(queryResponse);

        try {
            handler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected AthenaConnectorException for missing Timestream type");
        }
        catch (AthenaConnectorException e) {
            assertNotNull(e.getMessage());
            assertTrue("Message should mention missing type information", e.getMessage().contains("has no type information"));
            assertTrue("Message should mention the column name", e.getMessage().contains("missing_type"));
        }
    }

    @Test
    public void doGetQueryPassthroughSchema_emptyType_throwsException()
            throws Exception
    {
        Map<String, String> queryPassthroughArgs = new HashMap<>();
        queryPassthroughArgs.put(SCHEMA_FUNCTION_NAME, TimestreamQueryPassthrough.SCHEMA_NAME + "." + TimestreamQueryPassthrough.NAME);

        queryPassthroughArgs.put(TimestreamQueryPassthrough.QUERY,
                "SELECT id, my_time_series FROM \"db\".\"table\"");

        GetTableRequest request = new GetTableRequest(identity, "query-id", "default",
                new TableName("system", "query"), queryPassthroughArgs);

        Type emptyType = Type.builder().build();
        ColumnInfo badColumn = ColumnInfo.builder().name("my_time_series").type(emptyType).build();
        QueryResponse queryResponse = QueryResponse.builder()
                .columnInfo(Collections.singletonList(badColumn))
                .rows(Collections.emptyList())
                .build();

        when(mockTsQuery.query(nullable(QueryRequest.class))).thenReturn(queryResponse);

        try {
            handler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected AthenaConnectorException for empty Timestream type");
        }
        catch (AthenaConnectorException e) {
            assertNotNull(e.getMessage());
            assertTrue("Message should mention mapping failure", e.getMessage().contains("could not map"));
            assertTrue("Message should mention the column name", e.getMessage().contains("my_time_series"));
            assertTrue("Message should mention docs link", e.getMessage().contains("connectors-timestream.html"));
        }
    }
}
