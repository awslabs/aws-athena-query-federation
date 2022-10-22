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
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.vertica.query.QueryFactory;
import com.amazonaws.athena.connectors.vertica.query.VerticaExportQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import io.netty.util.internal.StringUtil;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.sql.*;
import java.util.*;


public class VerticaMetadataHandler
        extends MetadataHandler
{

    private static final Logger logger = LoggerFactory.getLogger(VerticaMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "vertica";
    protected static final String VERTICA_CONN_STR = "conn_str";
    private static final String DEFAULT_VERTICA = "default_vertica";
    private  static  final String TABLE_NAME = "TABLE_NAME";
    private static  final String TABLE_SCHEMA = "TABLE_SCHEM";
    private static final String[] TABLE_TYPES = {"TABLE"};
    private static final String EXPORT_BUCKET_KEY = "export_bucket";
    private static final String EMPTY_STRING = StringUtil.EMPTY_STRING;
    private final VerticaConnectionFactory connectionFactory;
    private final QueryFactory queryFactory = new QueryFactory();
    private final VerticaSchemaUtils verticaSchemaUtils;
    private AmazonS3 amazonS3;


    public VerticaMetadataHandler()
    {
        super(SOURCE_TYPE);
        amazonS3 = AmazonS3ClientBuilder.defaultClient();
        connectionFactory = new VerticaConnectionFactory();
        verticaSchemaUtils = new VerticaSchemaUtils();


    }

    @VisibleForTesting
    protected VerticaMetadataHandler(EncryptionKeyFactory keyFactory,
                                     VerticaConnectionFactory connectionFactory,
                                     AWSSecretsManager awsSecretsManager,
                                     AmazonAthena athena,
                                     String spillBucket,
                                     String spillPrefix,
                                     VerticaSchemaUtils verticaSchemaUtils,
                                     AmazonS3 amazonS3
                                     )
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.connectionFactory = connectionFactory;
        this.verticaSchemaUtils = verticaSchemaUtils;
        this.amazonS3 = amazonS3;

    }


    private Connection getConnection(MetadataRequest request) {
        String endpoint = resolveSecrets(getConnStr(request));
        return connectionFactory.getOrCreateConn(endpoint);

    }
    private String getConnStr(MetadataRequest request)
    {
        String conStr = System.getenv(request.getCatalogName());
        if (conStr == null) {
            logger.info("getConnStr: No environment variable found for catalog {} , using default {}",
                    request.getCatalogName(), DEFAULT_VERTICA);
            conStr = System.getenv(DEFAULT_VERTICA);
        }
        logger.info("exit getConnStr in VerticaMetadataHandler with conStr as {}",conStr);
        return conStr;
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
            throws SQLException
    {
        logger.info("doListSchemaNames: " + request.getCatalogName());
        List<String> schemas = new ArrayList<>();
        try (Connection client = getConnection(request)) {
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

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            throws SQLException
    {
        logger.info("doListTables: " + request);
        List<TableName> tables = new ArrayList<>();
        try (Connection client = getConnection(request)) {
            DatabaseMetaData dbMetadata = client.getMetaData();
            ResultSet table = dbMetadata.getTables(null, request.getSchemaName(),null, TABLE_TYPES);
            while (table.next()){
                tables.add(new TableName(table.getString(TABLE_SCHEMA), table.getString(TABLE_NAME)));
            }
        }
        return new ListTablesResponse(request.getCatalogName(), tables, null);

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
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: " + request.getTableName());
        Set<String> partitionCols = new HashSet<>();
        Connection connection = getConnection(request);

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
        public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws SQLException {
        logger.info("in getPartitions: "+ request);

        Schema schemaName = request.getSchema();
        TableName tableName = request.getTableName();
        Constraints constraints  = request.getConstraints();
        //get the bucket where export results wll be uploaded
        String s3ExportBucket = getS3ExportBucket();
        //Appending a random int to the query id to support multiple federated queries within a single query

        String randomStr = UUID.randomUUID().toString();
        String queryID = request.getQueryId().replace("-","").concat(randomStr);

        //Build the SQL query
        Connection connection = getConnection(request);
        DatabaseMetaData dbMetadata = connection.getMetaData();
        ResultSet definition = dbMetadata.getColumns(null, tableName.getSchemaName(), tableName.getTableName(), null);

        VerticaExportQueryBuilder queryBuilder = queryFactory.createVerticaExportQueryBuilder();

        String preparedSQLStmt = queryBuilder.withS3ExportBucket(s3ExportBucket)
                .withQueryID(queryID)
                .withColumns(definition, schemaName)
                .fromTable(tableName.getSchemaName(), tableName.getTableName())
                .withConstraints(constraints, schemaName)
                .build();

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
            throws SQLException
    {
        //ToDo: implement use of a continuation token to use in case of larger queries

        Connection connection = getConnection(request);
        Set<Split> splits = new HashSet<>();
        String exportBucket = getS3ExportBucket();
        String queryId = request.getQueryId().replace("-","");

        //testing if the user has access to the requested table
        testAccess(connection, request.getTableName());

        //get the SQL statement which was created in getPartitions
        FieldReader fieldReaderPS = request.getPartitions().getFieldReader("preparedStmt");
        String sqlStatement  = fieldReaderPS.readText().toString();
        String catalogName = request.getCatalogName();

        FieldReader fieldReaderQid = request.getPartitions().getFieldReader("queryId");
        String queryID  = fieldReaderQid.readText().toString();

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
                        .add(VERTICA_CONN_STR, getConnStr(request))
                        .add("exportBucket", exportBucket)
                        .add("s3ObjectKey", objectSummary.getKey())
                        .build();
                splits.add(split);

            }
            logger.info("doGetSplits: exit - " + splits.size());
            return new GetSplitsResponse(catalogName, splits);
        }
        else
            {
                //No records were exported by Vertica for the issued query, creating a "empty" split
                logger.info("No records were exported by Vertica");
                split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add("query_id", queryID)
                        .add(VERTICA_CONN_STR, getConnStr(request))
                        .add("exportBucket", exportBucket)
                        .add("s3ObjectKey", EMPTY_STRING)
                        .build();
                splits.add(split);
                logger.info("doGetSplits: exit - " + splits.size());
                return new GetSplitsResponse(catalogName,split);
            }

    }

    /*
     * Generates the necessary Prepared Statements to set the AWS Auth and export S3 bucket Region on Vertica
     * and executes the queries
     */
    private void executeQueriesOnVertica(Connection connection, String sqlStatement, String awsRegionSql)
            throws SQLException
    {
        PreparedStatement setAwsRegion = connection.prepareStatement(awsRegionSql);
        PreparedStatement exportSQL = connection.prepareStatement(sqlStatement);

        //execute the query to set region
        setAwsRegion.execute();

        //execute the query to export the data to S3
        exportSQL.execute();
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
       return System.getenv(EXPORT_BUCKET_KEY);
    }

}
