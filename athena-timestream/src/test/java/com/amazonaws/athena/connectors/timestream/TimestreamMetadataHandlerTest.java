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
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
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
public class TimestreamMetadataHandlerTest {
    private static final Logger logger = LoggerFactory.getLogger(TimestreamMetadataHandlerTest.class);
    
    private static final String DEFAULT_CATALOG = "default";
    private static final String QUERY_ID = "queryId";
    private static final String QUERY_ID_WITH_DASH = "query-id";
    private static final String TABLE_NAME_1 = "table1";
    private static final String TABLE_NAME_2 = "table2";
    private static final String TABLE_NAME_CASE_MISMATCH = "Table1";
    private static final String DATABASE_NAME_CASE_MISMATCH = "Default";
    private static final String DATABASE_NAME_1 = "database1";
    private static final String TABLE_NOT_FOUND_IN_GLUE = "Table not found in Glue";
    private static final String DATABASE_NOT_FOUND = "Database not found";
    private static final String TABLE_NOT_FOUND = "Table not found";
    private static final String SELECT_COL1_COL2_FROM_TABLE1 = "SELECT col1, col2 FROM table1";
    private static final String SELECT_STAR_FROM_TABLE1 = "SELECT * FROM table1";
    private static final String EXCEEDED_MAXIMUM_RESULT_SIZE = "Exceeded maximum result size";
    private static final String NO_QUERY_PASSED_THROUGH = "No Query passed through";
    private static final String UNEXPECTED_DATUM_SIZE = "Unexpected datum size";
    private static final String SYSTEM_SCHEMA = "system";
    private static final String QUERY_TABLE = "query";
    private static final String SYSTEM_QUERY_FUNCTION = "SYSTEM.QUERY";
    private static final String COLUMN_NAME_1 = "col1";
    private static final String COLUMN_NAME_2 = "col2";
    private static final String DATA_TYPE_VARCHAR = "varchar";
    private static final String DATA_TYPE_DOUBLE = "double";
    private static final String DATA_TYPE_DIMENSION = "dimension";
    private static final String DATA_TYPE_MEASURE_VALUE = "measure_value";
    private static final String PARTITION_ID_COLUMN = "partition_id";
    private static final String PAGINATION_TOKEN_1 = "token1";
    private static final String PAGINATION_TOKEN_PREFIX = "token";
    
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
    public void tearDown()
            throws Exception {
        if (allocator != null) {
            allocator.close();
        }
    }
    
    @Test
    public void doListSchemaNames_WhenDatabasesExist_ReturnsAllDatabaseNamesAsSchemas()
            throws Exception {
        logger.info("doListSchemaNames_WhenDatabasesExist_ReturnsAllDatabaseNamesAsSchemas - enter");
        
        when(mockTsMeta.listDatabases(nullable(ListDatabasesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListDatabasesRequest request = invocation.getArgument(0, ListDatabasesRequest.class);
            
            String newNextToken = null;
            List<Database> databases = new ArrayList<>();
            if (request.nextToken() == null) {
                for (int i = 0; i < 10; i++) {
                    databases.add(Database.builder().databaseName("database_" + i).build());
                }
                newNextToken = "1";
            } else if (request.nextToken().equals("1")) {
                for (int i = 10; i < 100; i++) {
                    databases.add(Database.builder().databaseName("database_" + i).build());
                }
                newNextToken = "2";
            } else if (request.nextToken().equals("2")) {
                for (int i = 100; i < 1000; i++) {
                    databases.add(Database.builder().databaseName("database_" + i).build());
                }
                newNextToken = null;
            }
            
            return ListDatabasesResponse.builder().databases(databases).nextToken(newNextToken).build();
        });
        
        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemaNames_WhenDatabasesExist_ReturnsAllDatabaseNamesAsSchemas - {}", res.getSchemas());
        
        assertEquals(1000, res.getSchemas().size());
        verify(mockTsMeta, times(3)).listDatabases(nullable(ListDatabasesRequest.class));
        Iterator<String> schemaItr = res.getSchemas().iterator();
        for (int i = 0; i < 1000; i++) {
            assertEquals("database_" + i, schemaItr.next());
        }
        
        logger.info("doListSchemaNames_WhenDatabasesExist_ReturnsAllDatabaseNamesAsSchemas - exit");
    }
    
    @Test
    public void doListTables_WhenTablesExist_ReturnsAllTableNamesInSchema()
            throws Exception {
        logger.info("doListTables_WhenTablesExist_ReturnsAllTableNamesInSchema - enter");
        
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
                    } else if (request.nextToken().equals("1")) {
                        for (int i = 10; i < 100; i++) {
                            tables.add(Table.builder().databaseName(request.databaseName()).tableName("table_" + i).build());
                        }
                        newNextToken = "2";
                    } else if (request.nextToken().equals("2")) {
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
        logger.info("doListTables_WhenTablesExist_ReturnsAllTableNamesInSchema - {}", res.getTables());
        
        assertEquals(1000, res.getTables().size());
        verify(mockTsMeta, times(3))
                .listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class));
        
        Iterator<TableName> schemaItr = res.getTables().iterator();
        for (int i = 0; i < 1000; i++) {
            TableName tableName = schemaItr.next();
            assertEquals(defaultSchema, tableName.getSchemaName());
            assertEquals("table_" + i, tableName.getTableName());
        }
        
        logger.info("doListTables_WhenTablesExist_ReturnsAllTableNamesInSchema - exit");
    }
    
    @Test
    public void doGetTable_WhenTableExistsInTimestream_ReturnsSchemaFromDescribeQuery()
            throws Exception {
        logger.info("doGetTable_WhenTableExistsInTimestream_ReturnsSchemaFromDescribeQuery - enter");
        
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
                QUERY_ID_WITH_DASH,
                DEFAULT_CATALOG,
                new TableName(defaultSchema, TABLE_NAME_1), Collections.emptyMap());
        
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable_WhenTableExistsInTimestream_ReturnsSchemaFromDescribeQuery - {}", res);
        
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
        
        logger.info("doGetTable_WhenTableExistsInTimestream_ReturnsSchemaFromDescribeQuery - exit");
    }
    
    @Test
    public void doGetTable_WhenTableExistsInGlue_ReturnsGlueSchemaWithViewMetadata()
            throws Exception {
        logger.info("doGetTable_WhenTableExistsInGlue_ReturnsGlueSchemaWithViewMetadata - enter");
        
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
                QUERY_ID_WITH_DASH,
                DEFAULT_CATALOG,
                new TableName(defaultSchema, TABLE_NAME_1), Collections.emptyMap());
        
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable_WhenTableExistsInGlue_ReturnsGlueSchemaWithViewMetadata - {}", res);
        
        assertEquals(2, res.getSchema().getFields().size());
        
        Field measureName = res.getSchema().findField("col1");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(measureName.getType()));
        
        Field measureValue = res.getSchema().findField("col2");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(measureValue.getType()));
        
        assertEquals("view text", res.getSchema().getCustomMetadata().get(VIEW_METADATA_FIELD));
        
        logger.info("doGetTable - exit");
    }
    
    @Test
    public void doGetTable_WhenTimeSeriesTableExistsInGlue_ReturnsTimeSeriesSchemaWithListViewMetadata()
            throws Exception {
        logger.info("doGetTable_WhenTimeSeriesTableExistsInGlue_ReturnsTimeSeriesSchemaWithListViewMetadata - enter");
        
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
        logger.info("doGetTable_WhenTimeSeriesTableExistsInGlue_ReturnsTimeSeriesSchemaWithListViewMetadata - {}", res);
        
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
        
        logger.info("doGetTable_WhenTimeSeriesTableExistsInGlue_ReturnsTimeSeriesSchemaWithListViewMetadata - exit");
    }
    
    @Test
    public void doGetTableLayout_WhenRequested_ReturnsSinglePartitionBlock()
            throws Exception {
        logger.info("doGetTableLayout_WhenRequested_ReturnsSinglePartitionBlock - enter");
        
        Schema schema = SchemaBuilder.newBuilder().build();
        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "query-id",
                defaultSchema,
                new TableName("database1", "table1"),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                Collections.EMPTY_SET);
        
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);
        
        logger.info("doGetTableLayout_WhenRequested_ReturnsSinglePartitionBlock - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout_WhenRequested_ReturnsSinglePartitionBlock:{} {}", row, BlockUtils.rowToString(partitions, row));
        }
        
        assertTrue(partitions.getRowCount() == 1);
        logger.info("doGetTableLayout_WhenRequested_ReturnsSinglePartitionBlock - exit");
    }
    
    @Test
    public void doGetSplits_WhenRequested_ReturnsOneSplitWithNullContinuationToken()
            throws Exception {
        logger.info("doGetSplits_WhenRequested_ReturnsOneSplitWithNullContinuationToken - enter");
        
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
                new Object[]{continuationToken, response.getSplits().size()});
        
        assertTrue("Continuation criteria violated", response.getSplits().size() == 1);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);
        
        logger.info("doGetSplits - exit");
    }
    
    @Test
    public void doGetDataSourceCapabilities_WhenCalled_ReturnsCatalogNameAndCapabilities() {
        GetDataSourceCapabilitiesRequest req = new GetDataSourceCapabilitiesRequest(identity, QUERY_ID, DEFAULT_CATALOG);
        GetDataSourceCapabilitiesResponse res = handler.doGetDataSourceCapabilities(allocator, req);
        
        assertNotNull("GetDataSourceCapabilities response should not be null", res);
        assertEquals("Catalog name should match request", DEFAULT_CATALOG, res.getCatalogName());
        assertNotNull("Capabilities should not be null", res.getCapabilities());
    }
    
    @Test
    public void doListTables_WithResourceNotFoundExceptionForExactName_ReturnsTablesFromCaseInsensitiveMatch()
            throws Exception {
        when(mockTsMeta.listDatabases(nullable(ListDatabasesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            List<Database> databases = new ArrayList<>();
            databases.add(Database.builder().databaseName(DATABASE_NAME_CASE_MISMATCH).build()); // Case mismatch
            return ListDatabasesResponse.builder().databases(databases).nextToken(null).build();
        });
        
        when(mockTsMeta.listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest request =
                            invocation.getArgument(0, software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class);
                    
                    // First call throws ResourceNotFoundException, second call succeeds
                    if (request.databaseName().equals(DEFAULT_CATALOG)) {
                        throw software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException.builder()
                                .message(DATABASE_NOT_FOUND)
                                .build();
                    }
                    
                    List<Table> tables = new ArrayList<>();
                    tables.add(Table.builder().databaseName(request.databaseName()).tableName(TABLE_NAME_1).build());
                    return software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse.builder()
                            .tables(tables).nextToken(null).build();
                });
        
        ListTablesRequest req = new ListTablesRequest(identity, QUERY_ID, DEFAULT_CATALOG, DEFAULT_CATALOG,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        
        assertEquals("Should return one table after case-insensitive fallback", 1, res.getTables().size());
        TableName firstTable = res.getTables().iterator().next();
        assertEquals("Schema name should match case-insensitive database", DATABASE_NAME_CASE_MISMATCH, firstTable.getSchemaName());
        assertEquals("Table name should match", TABLE_NAME_1, firstTable.getTableName());
    }

    @Test
    public void doGetQueryPassthroughSchema_scalarColumns_returnsSchema()
            throws Exception {
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
    public void doListTables_WithLimitedPageSize_ReturnsOneTableAndNextToken()
            throws Exception {
        when(mockTsMeta.listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest request =
                            invocation.getArgument(0, software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class);
                    
                    List<Table> tables = new ArrayList<>();
                    
                    tables.add(Table.builder().databaseName(request.databaseName()).tableName(TABLE_NAME_1).build());
                    return software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse.builder()
                            .tables(tables).nextToken(PAGINATION_TOKEN_1).build();
                    
                });
        
        ListTablesRequest req = new ListTablesRequest(identity, QUERY_ID, DEFAULT_CATALOG, defaultSchema,
                null, 10); // Limited page size
        ListTablesResponse res = handler.doListTables(allocator, req);
        
        assertEquals("Should return one table when page size is limited", 1, res.getTables().size());
        assertNotNull("Next token should be present when more pages exist", res.getNextToken());
    }
    
    
    @Test(expected = RuntimeException.class)
    public void doListTables_ExceedsMaxResults_ThrowsRuntimeException()
            throws Exception {
        final int[] callCount = {0};
        when(mockTsMeta.listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest request =
                            invocation.getArgument(0, software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class);
                    
                    List<Table> tables = new ArrayList<>();
                    // Return 50,001 tables per page to exceed MAX_RESULTS (100,000) on the third page
                    // First page: 0-50000, Second page: 50001-100000, Third page: 100001+ (exceeds limit)
                    int startIndex = callCount[0] * 50001;
                    int endIndex = Math.min(startIndex + 50001, 100002); // Ensure we exceed 100,000
                    for (int i = startIndex; i < endIndex; i++) {
                        tables.add(Table.builder().databaseName(request.databaseName()).tableName("table_" + i).build());
                    }
                    callCount[0]++;
                    // Keep returning tokens until we exceed MAX_RESULTS
                    String nextToken = (callCount[0] < 3) ? PAGINATION_TOKEN_PREFIX + callCount[0] : null;
                    return software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse.builder()
                            .tables(tables).nextToken(nextToken).build();
                });
        
        ListTablesRequest req = new ListTablesRequest(identity, QUERY_ID, DEFAULT_CATALOG, defaultSchema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        
        handler.doListTables(allocator, req);
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
    public void doGetTable_WithExactNameNotFoundInGlue_FindsTableCaseInsensitivelyAndReturnsSchemaWithOneField()
            throws Exception {
        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenThrow(new RuntimeException(TABLE_NOT_FOUND_IN_GLUE));
        
        // First call throws ValidationException, then we find the table with case-insensitive lookup
        when(mockTsMeta.listDatabases(nullable(ListDatabasesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            List<Database> databases = new ArrayList<>();
            databases.add(Database.builder().databaseName(DATABASE_NAME_CASE_MISMATCH).build()); // Case mismatch
            return ListDatabasesResponse.builder().databases(databases).nextToken(null).build();
        });
        
        when(mockTsMeta.listTables(nullable(software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest request =
                            invocation.getArgument(0, software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.class);
                    List<Table> tables = new ArrayList<>();
                    tables.add(Table.builder().databaseName(request.databaseName()).tableName(TABLE_NAME_CASE_MISMATCH).build()); // Case mismatch - different from TABLE_NAME_1
                    return software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse.builder()
                            .tables(tables).nextToken(null).build();
                });
        
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            QueryRequest request = invocation.getArgument(0, QueryRequest.class);
            // First call throws ValidationException
            if (request.queryString().contains("\"default\"")) {
                throw software.amazon.awssdk.services.timestreamquery.model.ValidationException.builder()
                        .message(TABLE_NOT_FOUND)
                        .build();
            }
            // Second call succeeds
            List<Row> rows = new ArrayList<>();
            rows.add(Row.builder().data(Datum.builder().scalarValue(COLUMN_NAME_1).build(),
                    Datum.builder().scalarValue(DATA_TYPE_VARCHAR).build(),
                    Datum.builder().scalarValue(DATA_TYPE_DIMENSION).build()).build());
            return QueryResponse.builder().rows(rows).build();
        });
        
        GetTableRequest req = new GetTableRequest(identity,
                QUERY_ID_WITH_DASH,
                DEFAULT_CATALOG,
                new TableName(DEFAULT_CATALOG, TABLE_NAME_1), Collections.emptyMap());
        
        GetTableResponse res = handler.doGetTable(allocator, req);
        assertNotNull("GetTable response should not be null after case-insensitive fallback", res);
        assertEquals("Schema should have one field", 1, res.getSchema().getFields().size());
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
    public void doGetQueryPassthroughSchema_WithValidQuery_ReturnsSchemaWithTwoColumns()
            throws Exception {
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY_FUNCTION);
        qptArgs.put(TimestreamQueryPassthrough.QUERY, SELECT_COL1_COL2_FROM_TABLE1);
        
        GetTableRequest req = new GetTableRequest(identity,
                QUERY_ID_WITH_DASH,
                DEFAULT_CATALOG,
                new TableName(SYSTEM_SCHEMA, QUERY_TABLE),
                qptArgs);
        
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            QueryRequest request = invocation.getArgument(0, QueryRequest.class);
            assertEquals("Query string should match passthrough query", SELECT_COL1_COL2_FROM_TABLE1, request.queryString());
            assertEquals("maxRows should be 1 for schema probe", 1, request.maxRows().intValue());
            
            List<ColumnInfo> columnInfos = new ArrayList<>();
            columnInfos.add(ColumnInfo.builder()
                    .name(COLUMN_NAME_1)
                    .type(Type.builder().scalarType(ScalarType.VARCHAR).build())
                    .build());
            columnInfos.add(ColumnInfo.builder()
                    .name(COLUMN_NAME_2)
                    .type(Type.builder().scalarType(ScalarType.DOUBLE).build())
                    .build());
            
            return QueryResponse.builder()
                    .columnInfo(columnInfos)
                    .rows(Collections.emptyList())
                    .build();
        });
        
        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);
        assertNotNull("GetTable response for query passthrough schema should not be null", res);
        assertEquals("Schema should have two columns", 2, res.getSchema().getFields().size());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void doGetQueryPassthroughSchema_WithoutQueryPassthrough_ThrowsIllegalArgumentException()
            throws Exception {
        GetTableRequest req = new GetTableRequest(identity,
                QUERY_ID_WITH_DASH,
                DEFAULT_CATALOG,
                new TableName(SYSTEM_SCHEMA, QUERY_TABLE),
                Collections.emptyMap()); // Empty map means not query passthrough
        
        handler.doGetQueryPassthroughSchema(allocator, req);
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
    public void getPartitions_WhenCalled_DoesNotThrow()
            throws Exception {
        Schema schema = SchemaBuilder.newBuilder().build();
        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                QUERY_ID_WITH_DASH,
                defaultSchema,
                new TableName(DATABASE_NAME_1, TABLE_NAME_1),
                TestUtils.constraints(Collections.emptyMap(), Collections.emptyMap()),
                schema,
                Collections.<String>emptySet());
        
        // This is a NoOp method, so we just verify it doesn't throw
        handler.getPartitions(null, req, null);
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
    
    @Test
    public void doGetSplits_WithQueryPassthroughArgs_ReturnsOneSplitWithQueryInProperties()
            throws Exception {
        List<String> partitionCols = new ArrayList<>();
        Block partitions = BlockUtils.newBlock(allocator, PARTITION_ID_COLUMN, Types.MinorType.INT.getType(), 0);
        
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(TimestreamQueryPassthrough.QUERY, SELECT_STAR_FROM_TABLE1);
        
        GetSplitsRequest req = new GetSplitsRequest(identity,
                QUERY_ID_WITH_DASH,
                defaultSchema,
                new TableName(DATABASE_NAME_1, TABLE_NAME_1),
                partitions,
                partitionCols,
                TestUtils.constraints(Collections.emptyMap(), qptArgs),
                null);
        
        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        assertNotNull("GetSplits response should not be null", response);
        assertEquals("Should return exactly one split for query passthrough", 1, response.getSplits().size());
        Split firstSplit = response.getSplits().iterator().next();
        assertNotNull("Split properties should not be null", firstSplit.getProperties());
        assertTrue("Split properties should contain QUERY key for passthrough", firstSplit.getProperties().containsKey(TimestreamQueryPassthrough.QUERY));
    }
    
    @Test(expected = RuntimeException.class)
    public void doGetTable_WithUnexpectedDatumSize_ThrowsRuntimeException()
            throws Exception {
        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenThrow(new RuntimeException(TABLE_NOT_FOUND_IN_GLUE));
        
        when(mockTsQuery.query(nullable(QueryRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            // Return a row with unexpected datum size (not 3)
            List<Datum> datum = new ArrayList<>();
            datum.add(Datum.builder().scalarValue(COLUMN_NAME_1).build());
            datum.add(Datum.builder().scalarValue(DATA_TYPE_VARCHAR).build());
            // Missing third element
            
            List<Row> rows = new ArrayList<>();
            rows.add(Row.builder().data(datum).build());
            return QueryResponse.builder().rows(rows).build();
        });
        
        GetTableRequest req = new GetTableRequest(identity,
                QUERY_ID_WITH_DASH,
                DEFAULT_CATALOG,
                new TableName(defaultSchema, TABLE_NAME_1), Collections.emptyMap());
        
        handler.doGetTable(allocator, req);
    }
}
