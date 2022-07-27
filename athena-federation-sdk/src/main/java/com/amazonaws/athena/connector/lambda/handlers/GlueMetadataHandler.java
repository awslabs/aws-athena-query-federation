package com.amazonaws.athena.connector.lambda.handlers;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

/**
 * This class allows you to leverage AWS Glue's DataCatalog to satisfy portions of the functionality required in a
 * MetadataHandler. More precisely, this implementation uses AWS Glue's DataCatalog to implement:
 * <p><ul>
 * <li>doListSchemas(...)
 * <li>doListTables(...)
 * <li>doGetTable(...)
 * </ul><p>
 * When you extend this class you can optionally provide a DatabaseFilter and/or TableFilter to decide which Databases
 * (aka schemas) and Tables are eligible for use with your connector. You can find examples of this in the
 * athena-hbase and athena-docdb connector modules. A common reason for this is when you happen to have databases/tables
 * in Glue which match the names of databases and tables in your source but that aren't actually relevant. In such cases
 * you may choose to ignore those Glue tables.
 * <p>
 * At present this class does not retrieve partition information from AWS Glue's DataCatalog. There is an open task
 * for how best to handle partitioning information in this class: https://github.com/awslabs/aws-athena-query-federation/issues/5
 * It is unclear at this time how many sources will have meaningful partition info in Glue but many sources (DocDB, Hbase, Redis)
 * benefited from having basic schema information in Glue. As a result we punted support for partition information to
 * a later time.
 *
 * @note All schema names, table names, and column names must be lower case at this time. Any entities that are uppercase or
 * mixed case will not be accessible in queries and will be lower cased by Athena's engine to ensure consistency across
 * sources. As such you may need to handle this when integrating with a source that supports mixed case. As an example,
 * you can look at the CloudwatchTableResolver in the athena-cloudwatch module for one potential approach to this challenge.
 * @see MetadataHandler
 */
public abstract class GlueMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(GlueMetadataHandler.class);

    /**
     * The maximum number of tables returned in a single response (as defined in the Glue API docs).
     */
    protected static final int GET_TABLES_REQUEST_MAX_RESULTS = 100;
    //name of the environment variable that can be used to set which Glue catalog to use (e.g. setting this to
    //a different aws account id allows you to use cross-account catalogs)
    private static final String CATALOG_NAME_ENV_OVERRIDE = "glue_catalog";
    //This is to override the connection timeout on the Glue client.
    //The default is 10 seconds, which when retried is 40 seconds.
    //Lower to 250 ms, 1 second with retry.
    private static final int CONNECT_TIMEOUT = 250;
    //Splitter for inline map properties
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().withKeyValueSeparator("=");
    //Regex we expect for a table resource ARN
    private static final Pattern TABLE_ARN_REGEX = Pattern.compile("^arn:(?:aws|aws-cn|aws-us-gov):[a-z]+:[a-z1-9-]+:[0-9]{12}:table\\/(.+)$");
    //Regex we expect for a lambda function ARN
    private static final String FUNCTION_ARN_REGEX = "arn:aws[a-zA-Z-]*?:lambda:[a-zA-Z0-9-]+:(\\d{12}):function:[a-zA-Z0-9-_]+";
    //Table property that we expect to contain the source table name
    public static final String SOURCE_TABLE_PROPERTY = "sourceTable";
    //Table property that we expect to contain the column name mapping
    public static final String COLUMN_NAME_MAPPING_PROPERTY = "columnMapping";
    // Table property (optional) that we expect to contain custom datetime formatting
    public static final String DATETIME_FORMAT_MAPPING_PROPERTY = "datetimeFormatMapping";
    // Table property (optional) that we will create from DATETIME_FORMAT_MAPPING_PROPERTY with normalized column names
    public static final String DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED = "datetimeFormatMappingNormalized";
    public static final String VIEW_METADATA_FIELD = "_view_template";

    // Metadata for indicating whether or not the glue table contained a set or decimal type.
    // This is needed because we did not used to support these types in the GlueLexer/Parser.
    // Now that we do, some connectors (like the DDB Connector) needs this information in order to
    // emulate behavior from prior versions.
    public static final String GLUE_TABLE_CONTAINS_PREVIOUSLY_UNSUPPORTED_TYPE = "glueTableContainsPreviouslyUnsupportedType";

    private final AWSGlue awsGlue;

    /**
     * Basic constructor which is recommended when extending this class.
     *
     * @param disable Whether to disable Glue usage. Useful for users that wish to rely on their handlers' schema inference.
     * @param sourceType The source type, used in diagnostic logging.
     */
    public GlueMetadataHandler(boolean disable, String sourceType)
    {
        super(sourceType);
        if (disable) {
            //The current instance does not want to leverage Glue for metadata
            awsGlue = null;
        }
        else {
            awsGlue = AWSGlueClientBuilder.standard()
                    .withClientConfiguration(new ClientConfiguration().withConnectionTimeout(CONNECT_TIMEOUT))
                    .build();
        }
    }

    /**
     * Constructor that allows injection of a customized Glue client.
     *
     * @param awsGlue The glue client to use.
     * @param sourceType The source type, used in diagnostic logging.
     */
    public GlueMetadataHandler(AWSGlue awsGlue, String sourceType)
    {
        super(sourceType);
        this.awsGlue = awsGlue;
    }

    /**
     * Full DI constructor used mostly for testing
     *
     * @param awsGlue The glue client to use.
     * @param encryptionKeyFactory The EncryptionKeyFactory to use for spill encryption.
     * @param secretsManager The AWSSecretsManager client that can be used when attempting to resolve secrets.
     * @param athena The Athena client that can be used to fetch query termination status to fast-fail this handler.
     * @param spillBucket The S3 Bucket to use when spilling results.
     * @param spillPrefix The S3 prefix to use when spilling results.
     */
    @VisibleForTesting
    protected GlueMetadataHandler(AWSGlue awsGlue,
            EncryptionKeyFactory encryptionKeyFactory,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            String sourceType,
            String spillBucket,
            String spillPrefix)
    {
        super(encryptionKeyFactory, secretsManager, athena, sourceType, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
    }

    /**
     * Provides access to the Glue client if the extender should need it. This will return null if Glue
     * use is disabled.
     *
     * @return The AWSGlue client being used by this class, or null if disabled.
     */
    protected AWSGlue getAwsGlue()
    {
        return awsGlue;
    }

    /**
     * Provides access to the current AWS Glue DataCatalog being used by this class.
     *
     * @param request The request for which we'd like to resolve the catalog.
     * @return The glue catalog to use for the request.
     */
    protected String getCatalog(MetadataRequest request)
    {
        String override = System.getenv(CATALOG_NAME_ENV_OVERRIDE);
        if (override == null) {
            if (request.getContext() != null) {
                String functionArn = request.getContext().getInvokedFunctionArn();
                String functionOwner = getFunctionOwner(functionArn).orElse(null);
                if (functionOwner != null) {
                    logger.debug("Function Owner: " + functionOwner);
                    return functionOwner;
                }
            }
            return request.getIdentity().getAccount();
        }
        return override;
    }

    /**
     * Returns an unfiltered list of schemas (aka databases) from AWS Glue DataCatalog.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return The ListSchemasResponse which mostly contains the list of schemas (aka databases).
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        return doListSchemaNames(blockAllocator, request, null);
    }

    /**
     * Returns a list of schemas (aka databases) from AWS Glue DataCatalog with optional filtering.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @param filter The DatabaseFilter to apply to all schemas (aka databases) before adding them to the results list.
     * @return The ListSchemasResponse which mostly contains the list of schemas (aka databases).
     */
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request, DatabaseFilter filter)
            throws Exception
    {
        GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
        getDatabasesRequest.setCatalogId(getCatalog(request));

        List<String> schemas = new ArrayList<>();
        String nextToken = null;
        do {
            getDatabasesRequest.setNextToken(nextToken);
            GetDatabasesResult result = awsGlue.getDatabases(getDatabasesRequest);

            for (Database next : result.getDatabaseList()) {
                if (filter == null || filter.filter(next)) {
                    schemas.add(next.getName());
                }
            }

            nextToken = result.getNextToken();
        }
        while (nextToken != null);

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Returns an unfiltered list of tables from AWS Glue DataCatalog for the requested schema (aka database)
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return The ListTablesResponse which mostly contains the list of table names.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        return doListTables(blockAllocator, request, null);
    }

    /**
     * Returns a paginated list of tables from AWS Glue DataCatalog with optional filtering for the requested schema
     * (aka database).
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @param filter The TableFilter to apply to all tables before adding them to the results list.
     * @return The ListTablesResponse which mostly contains the list of table names.
     * @implNote A complete (un-paginated) list of tables should be returned if the request's pageSize is set to
     * ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE.
     */
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request, TableFilter filter)
            throws Exception
    {
        GetTablesRequest getTablesRequest = new GetTablesRequest();
        getTablesRequest.setCatalogId(getCatalog(request));
        getTablesRequest.setDatabaseName(request.getSchemaName());

        Set<TableName> tables = new HashSet<>();
        String nextToken = request.getNextToken();
        int pageSize = request.getPageSize();
        do {
            getTablesRequest.setNextToken(nextToken);
            if (pageSize != UNLIMITED_PAGE_SIZE_VALUE) {
                // Paginated requests will include the maxResults argument determined by the minimum value between the
                // pageSize and the maximum results supported by Glue (as defined in the Glue API docs).
                int maxResults = Math.min(pageSize, GET_TABLES_REQUEST_MAX_RESULTS);
                getTablesRequest.setMaxResults(maxResults);
                pageSize -= maxResults;
            }
            GetTablesResult result = awsGlue.getTables(getTablesRequest);

            for (Table next : result.getTableList()) {
                if (filter == null || filter.filter(next)) {
                    tables.add(new TableName(request.getSchemaName(), next.getName()));
                }
            }

            nextToken = result.getNextToken();
        }
        while (nextToken != null && (pageSize == UNLIMITED_PAGE_SIZE_VALUE || pageSize > 0));

        return new ListTablesResponse(request.getCatalogName(), tables, nextToken);
    }

    /**
     * Attempts to retrieve a Table (columns and properties) from AWS Glue for the request schema (aka database) and table
     * name with no fitlering.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse mostly containing the columns, their types, and any table properties for the requested table.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        return doGetTable(blockAllocator, request, null);
    }

    private boolean isPreviouslyUnsupported(String glueType, Field arrowField)
    {
        // For the set type we have to compare against the glueType String because they are represented as Lists in Arrow.
        boolean currentResult = arrowField.getType().getTypeID().equals(ArrowType.ArrowTypeID.Decimal) ||
            arrowField.getType().getTypeID().equals(ArrowType.ArrowTypeID.Map) ||
            glueType.contains("set<");

        if (!currentResult) {
            // Need to recursively check the arrowField inner types
            for (Field child : arrowField.getChildren()) {
                if (isPreviouslyUnsupported("", child)) {
                    return true;
                }
            }
        }
        return currentResult;
    }

    /**
     * Attempts to retrieve a Table (columns and properties) from AWS Glue for the request schema (aka database) and table
     * name with no filtering.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @param filter The TableFilter to apply to any matching table before generating the result.
     * @return A GetTableResponse mostly containing the columns, their types, and any table properties for the requested table.
     * @note This method throws a RuntimeException if not table matching the requested criteria (and filter) is found.
     */
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request, TableFilter filter)
            throws Exception
    {
        TableName tableName = request.getTableName();
        com.amazonaws.services.glue.model.GetTableRequest getTableRequest = new com.amazonaws.services.glue.model.GetTableRequest();
        getTableRequest.setCatalogId(getCatalog(request));
        getTableRequest.setDatabaseName(tableName.getSchemaName());
        getTableRequest.setName(tableName.getTableName());

        GetTableResult result = awsGlue.getTable(getTableRequest);
        Table table = result.getTable();

        if (filter != null && !filter.filter(table)) {
            throw new RuntimeException("No matching table found " + request.getTableName());
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        if (table.getParameters() != null) {
            table.getParameters()
                    .entrySet()
                    .forEach(next -> schemaBuilder.addMetadata(next.getKey(), next.getValue()));
        }

        //A column name mapping can be provided to get around restrictive Glue naming rules
        Map<String, String> columnNameMapping = getColumnNameMapping(table);
        Map<String, String> dateTimeFormatMapping = getDateTimeFormatMapping(table);
        Map<String, String> datetimeFormatMappingWithColumnName = new HashMap<>();

        Set<String> partitionCols = new HashSet<>();
        if (table.getPartitionKeys() != null) {
            partitionCols = table.getPartitionKeys()
                    .stream().map(next -> columnNameMapping.getOrDefault(next.getName(), next.getName())).collect(Collectors.toSet());
        }

        // partition columns should be added to the schema if they exist
        List<Column> allColumns = Stream.of(table.getStorageDescriptor().getColumns(), table.getPartitionKeys() == null ? new ArrayList<Column>() : table.getPartitionKeys())
                .flatMap(x -> x.stream())
                .collect(Collectors.toList());

        boolean glueTableContainsPreviouslyUnsupportedType = false;
        for (Column next : allColumns) {
            String rawColumnName = next.getName();
            String mappedColumnName = columnNameMapping.getOrDefault(rawColumnName, rawColumnName);
            // apply any type override provided in typeOverrideMapping from metadata
            // this is currently only used for timestamp with timezone support
            logger.info("Column {} with registered type {}", rawColumnName, next.getType());
            Field arrowField = convertField(mappedColumnName, next.getType());
            schemaBuilder.addField(arrowField);
            // Add non-null non-empty comments to metadata
            if (next.getComment() != null && !next.getComment().trim().isEmpty()) {
                schemaBuilder.addMetadata(mappedColumnName, next.getComment());
            }
            if (dateTimeFormatMapping.containsKey(rawColumnName)) {
                datetimeFormatMappingWithColumnName.put(mappedColumnName, dateTimeFormatMapping.get(rawColumnName));
            }

            // Indicate that we found a `set` or `decimal` type so that we can set this metadata on the schemaBuilder later on
            if (glueTableContainsPreviouslyUnsupportedType == false && isPreviouslyUnsupported(next.getType(), arrowField)) {
                glueTableContainsPreviouslyUnsupportedType = true;
            }
        }

        populateDatetimeFormatMappingIfAvailable(schemaBuilder, datetimeFormatMappingWithColumnName);

        populateSourceTableNameIfAvailable(table, schemaBuilder);

        if (table.getViewOriginalText() != null && !table.getViewOriginalText().isEmpty()) {
            schemaBuilder.addMetadata(VIEW_METADATA_FIELD, table.getViewOriginalText());
        }

        schemaBuilder.addMetadata(GLUE_TABLE_CONTAINS_PREVIOUSLY_UNSUPPORTED_TYPE, String.valueOf(glueTableContainsPreviouslyUnsupportedType));

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                schemaBuilder.build(),
                partitionCols);
    }

    /**
     * Maps a Glue field to an Apache Arrow Field.
     *
     * @param name The name of the field in Glue.
     * @param glueType The type of the field in Glue.
     * @return The corresponding Apache Arrow Field.
     * @note You can override this implementation to provide your own mappings.
     */
    protected Field convertField(String name, String glueType)
    {
        try {
            return GlueFieldLexer.lex(name, glueType);
        }
        catch (RuntimeException ex) {
            throw new RuntimeException("Error converting field[" + name + "] with type[" + glueType + "]", ex);
        }
    }

    public interface TableFilter
    {
        /**
         * Used to filter table results.
         *
         * @param table The table to evaluate.
         * @return True if the provided table should be in the result, False if not.
         */
        boolean filter(Table table);
    }

    public interface DatabaseFilter
    {
        /**
         * Used to filter database results.
         *
         * @param database The database to evaluate.
         * @return True if the provided database should be in the result, False if not.
         */
        boolean filter(Database database);
    }

    /**
     * Glue has strict table naming rules and may not be able to match the exact table name from the source. So this stores
     * the source table name in the schema metadata if necessary to ease lookup later. It looks for it in the following places:
     * <p><ul>
     * <li>A table property called {@value SOURCE_TABLE_PROPERTY}
     * <li>In StorageDescriptor.Location in the form of an ARN (e.g. arn:aws:dynamodb:us-east-1:012345678910:table/mytable)
     * </ul><p>
     * Override this method to fetch the source table name from somewhere else.
     *
     * @param table The Glue Table
     * @param schemaBuilder The schema being generated
     */
    protected static void populateSourceTableNameIfAvailable(Table table, SchemaBuilder schemaBuilder)
    {
        String sourceTableProperty = table.getParameters().get(SOURCE_TABLE_PROPERTY);
        if (sourceTableProperty != null) {
            // table property exists so nothing to do (assumes all table properties were already copied)
            return;
        }
        String location = table.getStorageDescriptor().getLocation();
        if (location != null) {
            Matcher matcher = TABLE_ARN_REGEX.matcher(location);
            if (matcher.matches()) {
                schemaBuilder.addMetadata(SOURCE_TABLE_PROPERTY, matcher.group(1));
            }
        }
    }

    /**
     * Will return the source table name stored by {@link #populateSourceTableNameIfAvailable}
     *
     * @param schema The schema returned by {@link #doGetTable}
     * @return The source table name
     */
    protected static String getSourceTableName(Schema schema)
    {
        return schema.getCustomMetadata().get(SOURCE_TABLE_PROPERTY);
    }

    /**
     * If available, will parse and return a column name mapping for cases when a data source's columns
     * cannot be represented by Glue's quite restrictive naming rules. It looks a comma separated inline map
     * in the {@value #COLUMN_NAME_MAPPING_PROPERTY} table property.
     *
     * @param table The glue table
     * @return A column mapping if provided, otherwise an empty map
     */
    protected static Map<String, String> getColumnNameMapping(Table table)
    {
        String columnNameMappingParam = table.getParameters().get(COLUMN_NAME_MAPPING_PROPERTY);
        if (!Strings.isNullOrEmpty(columnNameMappingParam)) {
            return MAP_SPLITTER.split(columnNameMappingParam);
        }
        return ImmutableMap.of();
    }

    /**
     * If available, Retrieves the map of normalized column names to customized format
     * for any string representation of date/datetime
     *
     * @param table The glue table
     * @returns a map of column name to date/datetime format that is used to parse the values in table
     * if provided, otherwise an empty map
     */
    private Map<String, String> getDateTimeFormatMapping(Table table)
    {
        String datetimeFormatMappingParam = table.getParameters().get(DATETIME_FORMAT_MAPPING_PROPERTY);
        if (!Strings.isNullOrEmpty(datetimeFormatMappingParam)) {
            return MAP_SPLITTER.split(datetimeFormatMappingParam);
        }
        return ImmutableMap.of();
    }

    /**
     * If available, adds stringified map of normalized columns to date/datetime format to Schema metadata
     *
     * @param schemaBuilder The schema being generated
     * @param dateTimeFormatMapping map of normalized column names to date/datetime format, if provided
     */
    private void populateDatetimeFormatMappingIfAvailable(SchemaBuilder schemaBuilder,
            Map<String, String> dateTimeFormatMapping)
    {
        if (dateTimeFormatMapping.size() > 0) {
            String datetimeFormatMappingString = dateTimeFormatMapping.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining(","));
            schemaBuilder.addMetadata(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED, datetimeFormatMappingString);
        }
    }

    /**
     * Parse the function owner from a lambda function ARN
     *
     * @param functionArn The lambda function arn
     * @returns a string of the function owner
     */
    private Optional<String> getFunctionOwner(String functionArn)
    {
        if (functionArn != null) {
            Pattern arnPattern = Pattern.compile(FUNCTION_ARN_REGEX);
            Matcher arnMatcher = arnPattern.matcher(functionArn);
            try {
                if (arnMatcher.matches() && arnMatcher.groupCount() > 0 && arnMatcher.group(1) != null) {
                    return Optional.of(arnMatcher.group(1));
                }
            }
            catch (Exception e) {
                logger.warn("Unable to parse owner from function arn: " + functionArn, e);
            }
        }
        return Optional.empty();
    }
}
