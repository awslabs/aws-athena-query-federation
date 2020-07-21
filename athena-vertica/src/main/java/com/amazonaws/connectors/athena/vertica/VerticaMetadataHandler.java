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

package com.amazonaws.connectors.athena.vertica;

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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


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
    private static final String VERTICA_QUOTE_CHARACTER = "\"";
    private static final String AWS_SECRETS = "aws_secret_name";
    private static final String DEFAULT_REGION = "aws_region";
    private final VerticaConnectionFactory connectionFactory;
    private final VerticaJdbcSplitQueryBuilder verticaJdbcSplitQueryBuilder;


    public VerticaMetadataHandler()
    {
        super(SOURCE_TYPE);
        connectionFactory = new VerticaConnectionFactory();
        verticaJdbcSplitQueryBuilder = new VerticaJdbcSplitQueryBuilder(VERTICA_QUOTE_CHARACTER);

    }

    @VisibleForTesting
    protected VerticaMetadataHandler(EncryptionKeyFactory keyFactory,
                                     VerticaConnectionFactory connectionFactory,
                                     AWSSecretsManager awsSecretsManager,
                                     AmazonAthena athena,
                                     String spillBucket,
                                     String spillPrefix,
                                     VerticaJdbcSplitQueryBuilder verticaJdbcSplitQueryBuilder)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.connectionFactory = connectionFactory;
        this.verticaJdbcSplitQueryBuilder = verticaJdbcSplitQueryBuilder;
    }


    private Connection getOrCreateConn(MetadataRequest request) throws SQLException {
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
    {
        logger.info("doListSchemaNames: " + request.getCatalogName());

        //get schemas
        List<String> schemas = new ArrayList<>();
        try {
            Connection client = getOrCreateConn(request);
            DatabaseMetaData dbMetadata = client.getMetaData();
            ResultSet rs  = dbMetadata.getTables(null, null, null, TABLE_TYPES);;
            while (rs.next()) {
                if(!schemas.contains(rs.getString(TABLE_SCHEMA))) {
                    schemas.add(rs.getString(TABLE_SCHEMA));
                }
            }
        }
        catch (SQLFeatureNotSupportedException e){
           logger.info("SQL Feature Not Supported Exception"+ e.getMessage());
        }
        catch (SQLException e){
            logger.info("SQL Exception"+ e.getMessage());
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
    {
        logger.info("doListTables: " + request);

        List<TableName> tables = new ArrayList<>();
        try {
            Connection client = getOrCreateConn(request);
            DatabaseMetaData dbMetadata = client.getMetaData();
            ResultSet table = dbMetadata.getTables(null,request.getSchemaName(),null,TABLE_TYPES);
            while (table.next()){
                tables.add(new TableName(table.getString(TABLE_SCHEMA),table.getString(TABLE_NAME)));
            }
        }
        catch (SQLException e) {
            logger.info("SQL Exception"+ e.getMessage());
        }
        return new ListTablesResponse(request.getCatalogName(), tables);

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
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws SQLException {
        logger.info("doGetTable: " + request.getTableName());

        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();
        Set<String> partitionCols = new HashSet<>();

        try {
            Connection client = getOrCreateConn(request);
            DatabaseMetaData dbMetadata = client.getMetaData();
            TableName name = request.getTableName();
            ResultSet definition = dbMetadata.getColumns(null,name.getSchemaName(),name.getTableName(), null);
            while(definition.next()) {

                //TODO: this needs to change as per the schema of the table in Vertica; Need to add other data types here!!
                //If Integer
                if (definition.getString("TYPE_NAME").equalsIgnoreCase("Integer")) {
                    tableSchemaBuilder.addIntField(definition.getString("COLUMN_NAME"));
                }
                //If VARCHAR
                if (definition.getString("TYPE_NAME").equalsIgnoreCase("Varchar")) {
                    tableSchemaBuilder.addStringField(definition.getString("COLUMN_NAME"));
                }
                //If Numeric
                if (definition.getString("TYPE_NAME").equalsIgnoreCase("NUMERIC")) {
                    tableSchemaBuilder.addDecimalField(definition.getString("COLUMN_NAME"), 10, 2);
                }
            }

        }
        catch (SQLException e) {
           logger.info("SQL Exception in doGetTable : "+ e.getMessage());
        }
        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
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
        public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        logger.info("in getPartitions: "+ request);

        StringBuilder sqlStmt = new StringBuilder();
        Schema schemaName = request.getSchema();
        TableName tableName = request.getTableName();
        Constraints contr  = request.getConstraints();

        try {
            Connection connection = getOrCreateConn(request);

            //Generate the SQL from user provided query
            String colNames = schemaName.getFields().stream().map(Field::getName).collect(Collectors.joining(","));
            sqlStmt.append(colNames);

            String preparedStmt = verticaJdbcSplitQueryBuilder.buildSql(tableName.getSchemaName(), tableName.getTableName(), schemaName, contr, request.getQueryId());

            // write the prepared statement to the partition column created in enhancePartitionSchema
            blockWriter.writeRows((Block block, int rowNum) -> {
                boolean matched;
                matched = block.setValue("preparedStmt", rowNum, preparedStmt);
                return matched ? 1 : 0;
            });
        } catch (SQLException e) {
           logger.info("Exception in getPartitions: "+ e.getMessage());
        }


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

        logger.info("doGetSplits: " + request);

        Set<Split> splits = new HashSet<>();
        FieldReader fieldReader = request.getPartitions().getFieldReader("preparedStmt");

        String sqlStatement  = fieldReader.readText().toString();
        String catalogName = request.getCatalogName();

        /**
         * Build sql based on what query has been issued and concat it with the export to parquet command
         */

        try{
            Connection connection = getOrCreateConn(request);

            logger.info("Generated EXPORT statement is: " + sqlStatement);

            //get AWS KEYS from AWS Secrets Manager
            String awsSecrets = getSecret(System.getenv(AWS_SECRETS));

            //extracting Secrets
            String replacedSecretString = (awsSecrets.replace("{","")
                    .replace("}","")
                    .replace("\"","")
                    .replaceAll("\\s+",""));

            Map<String, String> result = Arrays.stream(replacedSecretString.split(","))
                    .map(k ->  k.split(":"))
                    .collect(Collectors.toMap(
                            a -> a[0],  //key
                            a -> a[1]   //value
                    ));

            String awsSecretKey = result.get("aws_secret_access_key");
            String awsAccessId = result.get("aws_access_key_id");

            //Generating the sql to set the AWS Session Config on Vertica
            StringBuilder sb = new StringBuilder();
            ST temp = new ST("ALTER SESSION SET AWSAuth='<access_key>:<secret_key>'");
            temp.add("access_key", awsAccessId);
            temp.add("secret_key", awsSecretKey);
            String credS = temp.render();

            PreparedStatement credPS = connection.prepareStatement(credS);
            PreparedStatement exportSQL = connection.prepareStatement(sqlStatement);

            // Set the AWS Region on Vertica
            String defaultRegion = System.getenv(DEFAULT_REGION).toLowerCase();
            String awsRegion =  "SELECT SET_CONFIG_PARAMETER('AWSRegion','"+defaultRegion+"') AS REGION_NAME";

            PreparedStatement setAwsRegion = connection.prepareStatement(awsRegion);

            //execute the query to set credentials
            credPS.execute();

            //execute the query to set Region
            ResultSet rSet = setAwsRegion.executeQuery();

            //execute the query to export the data to S3
            exportSQL.execute();

        } catch (SQLException e) {
            logger.info("Exception in doGetSplits executing the Prepared Statement " + e.getMessage());
        }

        //generating the file name
        String exportBucket = System.getenv("export_bucket");
        String s3File = "s3://" + exportBucket + "/" + request.getQueryId().replace("-","") + ".parquet";

        //creating the split
        Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                .add("query_id", request.getQueryId())
                .add(VERTICA_CONN_STR, getConnStr(request))
                .add("fileLocation",s3File)
                .add("exportBucket", exportBucket)
                .build();

        splits.add(split);

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);

    }


}
