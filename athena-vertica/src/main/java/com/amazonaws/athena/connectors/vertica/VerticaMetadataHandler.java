/*-
 * #%L
 * athena-vertica
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

package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.SdkClientException;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.athena.connectors.vertica.query.QueryFactory;
import com.amazonaws.athena.connectors.vertica.query.VerticaExportQueryBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_NAME;
import static com.amazonaws.athena.connectors.vertica.VerticaSchemaUtils.convertToArrowType;


public class VerticaMetadataHandler
        extends JdbcMetadataHandler
{

    private static final Logger logger = LoggerFactory.getLogger(VerticaMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    private static final String EXPORT_BUCKET_KEY = "export_bucket";
    private static final String EMPTY_STRING = StringUtils.EMPTY;
    private static final String TABLE_SCHEMA = "TABLE_SCHEM";
    private static final String[] TABLE_TYPES = {"TABLE"};
    private final QueryFactory queryFactory = new QueryFactory();
    private final VerticaSchemaUtils verticaSchemaUtils;
    private AmazonS3 amazonS3;

    private final JdbcQueryPassthrough queryPassthrough = new JdbcQueryPassthrough();

    public VerticaMetadataHandler(Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(VERTICA_NAME, configOptions), configOptions);
    }

    public VerticaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(VERTICA_DRIVER_CLASS, VERTICA_DEFAULT_PORT)), configOptions);
    }

    public VerticaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        amazonS3 = AmazonS3ClientBuilder.defaultClient();
        verticaSchemaUtils = new VerticaSchemaUtils();
    }
    @VisibleForTesting
    public VerticaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, Map<String, String> configOptions, AmazonS3 amazonS3, VerticaSchemaUtils verticaSchemaUtils)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.amazonS3 = amazonS3;
        this.verticaSchemaUtils = verticaSchemaUtils;
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        logger.info("doListSchemaNames: {}", request.getCatalogName());
        List<String> schemas = new ArrayList<>();
        try (Connection client = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            DatabaseMetaData dbMetadata = client.getMetaData();
            ResultSet rs  = dbMetadata.getTables(null, null, null, TABLE_TYPES);

            while (rs.next())
            {
                if(!schemas.contains(rs.getString(TABLE_SCHEMA)))
                {
                    schemas.add(rs.getString(TABLE_SCHEMA));
                }
            }
        }
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    protected ArrowType getArrayArrowTypeFromTypeName(String typeName, int precision, int scale)
    {
        // Default ARRAY type is VARCHAR.
        return new ArrowType.Utf8();
    }
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }


    @Override
    public GetTableResponse doGetQueryPassthroughSchema(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        if (!getTableRequest.isQueryPassthrough()) {
            throw new IllegalArgumentException("No Query passed through [{}]" + getTableRequest);
        }

        queryPassthrough.verify(getTableRequest.getQueryPassthroughArguments());
        String customerPassedQuery = getTableRequest.getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            PreparedStatement preparedStatement = connection.prepareStatement(customerPassedQuery);
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            if (metadata == null) {
                throw new UnsupportedOperationException("Query not supported: ResultSetMetaData not available for query: " + customerPassedQuery);
            }
            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

            for (int columnIndex = 1; columnIndex <= metadata.getColumnCount(); columnIndex++) {
                String columnName = metadata.getColumnName(columnIndex);
                String columnLabel = metadata.getColumnLabel(columnIndex);
                columnName = columnName.equals(columnLabel) ? columnName : columnLabel;
                convertToArrowType(schemaBuilder, columnName, metadata.getColumnTypeName(columnIndex));
            }

            Schema schema = schemaBuilder.build();
            return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(), schema, Collections.emptySet());
        }
    }

    @Override
    public Schema getPartitionSchema(String catalogName)
    {
        return null;
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws Exception
    {
        Set<String> partitionCols = new HashSet<>();
        Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());

        //build the schema as per columns in Vertica
        Schema schema = verticaSchemaUtils.buildTableSchema(connection, request.getTableName());

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                schema,
                partitionCols
        );
    }

    /**
     * Here we inject the additional column to hold the Prepared SQL Statement.
     *
     * @param partitionSchemaBuilder The SchemaBuilder you can use to add additional columns and metadata to the
     * partitions response.
     * @param request The GetTableLayoutResquest that triggered this call.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request) {

        logger.info("{}: Catalog {}, table {}", request.getQueryId(), request.getTableName().getSchemaName(), request.getTableName());
        partitionSchemaBuilder.addField("preparedStmt", new ArrowType.Utf8());
        partitionSchemaBuilder.addField("queryId", new ArrowType.Utf8());
        partitionSchemaBuilder.addField("awsRegionSql", new ArrowType.Utf8());
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     * Here generating the SQL from the request and attaching it as a additional column
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {

        Schema schemaName = request.getSchema();
        TableName tableName = request.getTableName();
        Constraints constraints  = request.getConstraints();
        //get the bucket where export results wll be uploaded
        String s3ExportBucket = getS3ExportBucket();
        //Appending a random int to the query id to support multiple federated queries within a single query

        String randomStr = UUID.randomUUID().toString();
        String queryID = request.getQueryId().replace("-","").concat(randomStr);

        //Build the SQL query
        Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());

        // if  QPT get input query from Athena console
        //else old logic

        VerticaExportQueryBuilder queryBuilder = queryFactory.createVerticaExportQueryBuilder();
        String preparedSQLStmt;

        if (!request.getTableName().getQualifiedTableName().equalsIgnoreCase(queryPassthrough.getFunctionSignature())) {

            DatabaseMetaData dbMetadata = connection.getMetaData();
            ResultSet definition = dbMetadata.getColumns(null, tableName.getSchemaName(), tableName.getTableName(), null);

            preparedSQLStmt = queryBuilder.withS3ExportBucket(s3ExportBucket)
                    .withQueryID(queryID)
                    .withColumns(definition, schemaName)
                    .fromTable(tableName.getSchemaName(), tableName.getTableName())
                    .withConstraints(constraints, schemaName)
                    .build();
        } else {
            preparedSQLStmt = null;
        }

        logger.info("Vertica Export Statement: {}", preparedSQLStmt);
        // Build the Set AWS Region SQL
        String awsRegionSql = queryBuilder.buildSetAwsRegionSql(amazonS3.getRegion().toString());

        // write the prepared SQL statement to the partition column created in enhancePartitionSchema
        blockWriter.writeRows((Block block, int rowNum) ->{
            boolean matched;
            matched = block.setValue("preparedStmt", rowNum, preparedSQLStmt);
            matched &= block.setValue("queryId", rowNum, queryID);
            matched &= block.setValue("awsRegionSql", rowNum, awsRegionSql);
            //If all fields matches then we wrote 1 row during this call so we return 1
            return matched ? 1 : 0;
        });

    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * Here we execute the SQL on Vertica
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        //ToDo: implement use of a continuation token to use in case of larger queries
        Connection connection;
        try {
            connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
        } catch (Exception e) {
            throw new RuntimeException("connection failed ", e);
        }
        Set<Split> splits = new HashSet<>();
        String exportBucket = getS3ExportBucket();
        String queryId = request.getQueryId().replace("-","");
        Constraints constraints  = request.getConstraints();
        String s3ExportBucket = getS3ExportBucket();
        String sqlStatement;
        //testing if the user has access to the requested table

        FieldReader fieldReaderQid = request.getPartitions().getFieldReader("queryId");
        String queryID  = fieldReaderQid.readText().toString();

        //get the SQL statement which was created in getPartitions
        FieldReader fieldReaderPS = request.getPartitions().getFieldReader("preparedStmt");
        if (constraints.isQueryPassThrough()) {
            String preparedSQL = buildQueryPassthroughSql(constraints);

            VerticaExportQueryBuilder queryBuilder = queryFactory.createQptVerticaExportQueryBuilder();
            sqlStatement = queryBuilder.withS3ExportBucket(s3ExportBucket)
                    .withQueryID(queryID)
                    .withPreparedStatementSQL(preparedSQL).build();
            logger.info("Vertica Export Statement: {}", sqlStatement);
        }
        else {
            testAccess(connection, request.getTableName());
            sqlStatement = fieldReaderPS.readText().toString();
        }
        String catalogName = request.getCatalogName();

        FieldReader fieldReaderAwsRegion = request.getPartitions().getFieldReader("awsRegionSql");
        String awsRegionSql  = fieldReaderAwsRegion.readText().toString();


        //execute the queries on Vertica
        executeQueriesOnVertica(connection, sqlStatement, awsRegionSql);

        /*
         * For each generated S3 object, create a split and add data to the split.
         */
        Split split;
        List<S3ObjectSummary> s3ObjectSummaries = getlistExportedObjects(exportBucket, queryId);

        if(!s3ObjectSummaries.isEmpty())
        {
            for (S3ObjectSummary objectSummary : s3ObjectSummaries)
            {
                split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add("query_id", queryID)
                        .add("exportBucket", exportBucket)
                        .add("s3ObjectKey", objectSummary.getKey())
                        .build();
                splits.add(split);

            }
            return new GetSplitsResponse(catalogName, splits);
        }
        else
        {
            //No records were exported by Vertica for the issued query, creating a "empty" split
            logger.info("No records were exported by Vertica");
            split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add("query_id", queryID)
                    .add("exportBucket", exportBucket)
                    .add("s3ObjectKey", EMPTY_STRING)
                    .build();
            splits.add(split);
            return new GetSplitsResponse(catalogName,split);
        }

    }

    /*
     * Generates the necessary Prepared Statements to set the AWS Auth and export S3 bucket Region on Vertica
     * and executes the queries
     */
    private void executeQueriesOnVertica(Connection connection, String sqlStatement, String awsRegionSql)
    {
        try {
            PreparedStatement setAwsRegion = connection.prepareStatement(awsRegionSql);
            PreparedStatement exportSQL = connection.prepareStatement(sqlStatement);

            //execute the query to set region
            setAwsRegion.execute();

            //execute the query to export the data to S3
            exportSQL.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Exception in executing query in vertica ", e);
        }
    }

    /*
     * Get the list of all the exported S3 objects
     */
    private List<S3ObjectSummary> getlistExportedObjects(String s3ExportBucket, String queryId){
        ObjectListing objectListing;
        try
        {
            objectListing = amazonS3.listObjects(new ListObjectsRequest().withBucketName(s3ExportBucket).withPrefix(queryId));
        }
        catch (SdkClientException e)
        {
            throw new RuntimeException("Exception listing the exported objects : " + e.getMessage(), e);
        }
        return objectListing.getObjectSummaries();
    }

    private void testAccess(Connection conn, TableName table) {
        ST simpleTestSqlST = new ST("SELECT * FROM <schemaName>.<tableName> Limit 1;");
        simpleTestSqlST.add("schemaName", table.getSchemaName());
        simpleTestSqlST.add("tableName", table.getTableName());
        logger.info("Checking if the user has access to {}.{}", table.getSchemaName(), table.getTableName());
        try {
            PreparedStatement testAccessSql = conn.prepareStatement(simpleTestSqlST.render());
            ResultSet resultSet = testAccessSql.executeQuery();
        } catch (Exception e) {
            if (e.getMessage().contains("Permission denied")) {
                throw new RuntimeException("Permission Denied: " + e.getMessage());
            } else if (e.getMessage().contains("Insufficient privilege")) {
                throw new RuntimeException("Insufficient privilege" + e.getMessage());
            } else {
                throw new RuntimeException(e.getMessage());
            }
        }
        logger.info("User has access to {}.{}", table.getSchemaName(), table.getTableName());


    }

    public String getS3ExportBucket()
    {
        return configOptions.get(EXPORT_BUCKET_KEY);
    }

    public String buildQueryPassthroughSql(Constraints constraints)
    {
        queryPassthrough.verify(constraints.getQueryPassthroughArguments());
        return  constraints.getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
    }

}
