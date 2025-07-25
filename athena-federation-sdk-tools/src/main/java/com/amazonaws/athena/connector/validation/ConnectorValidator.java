/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.validation;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.google.common.collect.Sets;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.rowToString;
import static com.amazonaws.athena.connector.validation.ConstraintParser.parseConstraints;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This class should be used to validate deployed Lambda functions that use Athena's Federation SDK.
 * It simulates the basic query patterns that Athena will send to Lambda throughout its usage so that
 * broader and sometimes more subtle logical issues can be discovered before being used through Athena.
 * <p>
 * You can run this tool using the following command:
 * mvn exec:java -Dexec.mainClass=com.amazonaws.athena.connector.validation.ConnectorValidator -Dexec.args="[args]"
 * <p>
 * This tool can also be run using the validate_connector.sh script in the tools directory under the package root:
 * tools/validate_connector.sh [args]
 */
public class ConnectorValidator
{
  private static final Logger log = LoggerFactory.getLogger(ConnectorValidator.class);

  private static final Random RAND = new Random();

  static final BlockAllocator BLOCK_ALLOCATOR = new BlockAllocatorImpl();

  private ConnectorValidator()
  {
    // Intentionally left blank.
  }

  /**
   * The main method of this class allows the following argument pattern:
   * --lambda-func lambda_func [--record-func record_func] [--catalog catalog]
   * [--schema schema [--table table [--constraints constraints]]] [--planning-only] [--help]
   * <p>
   * Run with the -h or --help options to see full argument descriptions, or see {@link TestConfig} below.
   */
  public static void main(String[] args)
  {
    try {
      TestConfig testConfig = TestConfig.fromArgs(args);

      /*
       * SHOW DATABASES
       */
      logTestQuery("SHOW DATABASES");
      Collection<String> schemas = showDatabases(testConfig);

      // SHOW TABLES
      final String db = testConfig.getSchemaId().isPresent()
                                ? testConfig.getSchemaId().get()
                                : getRandomElement(schemas);
      log.info("Using database {}", db);
      logTestQuery("SHOW TABLES IN " + db);
      Collection<TableName> tables = showTables(testConfig, db);

      /*
       * DESCRIBE TABLE
       */
      final TableName table = testConfig.getTableId().isPresent()
                                      ? new TableName(db, testConfig.getTableId().get())
                                      : getRandomElement(tables);
      log.info("Using table {}", toQualifiedTableName(table));
      logTestQuery("DESCRIBE " + toQualifiedTableName(table));
      GetTableResponse tableResponse = describeTable(testConfig, table);
      final Schema schema = tableResponse.getSchema();
      final Set<String> partitionColumns = tableResponse.getPartitionColumns();

      /*
       * SELECT
       */
      logTestQuery("SELECT * FROM " + toQualifiedTableName(table));
      GetTableLayoutResponse tableLayout = getTableLayout(testConfig, table, schema, partitionColumns);

      GetSplitsResponse splitsResponse = getSplits(testConfig,
                                                   table,
                                                   schema,
                                                   tableLayout,
                                                   partitionColumns,
                                                   null);

      if (!testConfig.isPlanningOnly()) {
        readRecords(testConfig, table, schema, splitsResponse.getSplits());
      }
      else {
        log.info("Skipping record reading because the arguments indicated that only the planning should be validated.");
      }

      if (splitsResponse.getContinuationToken() == null) {
        logSuccess();
        return;
      }

      log.info("GetSplits included a continuation token: " + splitsResponse.getContinuationToken());
      log.info("Testing next batch of splits.");

      splitsResponse = getSplits(testConfig,
                                 table,
                                 schema,
                                 tableLayout,
                                 partitionColumns,
                                 splitsResponse.getContinuationToken());

      if (!testConfig.isPlanningOnly()) {
        readRecords(testConfig, table, schema, splitsResponse.getSplits());
      }
      else {
        log.info("Skipping record reading because the arguments indicated that only the planning should be validated.");
      }

      logSuccess();
    }
    catch (Exception ex) {
      logFailure(ex);
      System.exit(1);
    }

    System.exit(0);
  }

  private static Collection<String> showDatabases(TestConfig testConfig)
  {
    ListSchemasResponse schemasResponse = LambdaMetadataProvider.listSchemas(testConfig.getCatalogId(),
                                                                             testConfig.getMetadataFunction(),
                                                                             testConfig.getIdentity());
    final Collection<String> schemas = schemasResponse.getSchemas();
    log.info("Found databases: " + schemas);
    requireNonNull(schemas, "Returned collection of schemas was null!");
    checkState(!schemas.isEmpty(), "No schemas were returned!");
    List<String> notLower = schemas.stream().filter(s -> !s.equals(s.toLowerCase())).collect(Collectors.toList());
    checkState(notLower.isEmpty(),
                             "All returned schemas must be lowercase! Found these non-lowercase schemas: " + notLower);
    return schemas;
  }

  private static Collection<TableName> showTables(TestConfig testConfig, String db)
  {
    ListTablesResponse tablesResponse = LambdaMetadataProvider.listTables(testConfig.getCatalogId(),
                                                                          db,
                                                                          testConfig.getMetadataFunction(),
                                                                          testConfig.getIdentity());
    final Collection<TableName> tables = tablesResponse.getTables();
    log.info("Found tables: " + tables.stream()
                                        .map(t -> toQualifiedTableName(t))
                                        .collect(Collectors.toList()));
    requireNonNull(tables, "Returned collection of tables was null!");
    if (!testConfig.isAllowEmptyTables()) {
      checkState(!tables.isEmpty(), "No tables were returned!");
      List<String> notLower = tables.stream().filter(t -> !t.equals(new TableName(t.getSchemaName().toLowerCase(),
              t.getTableName().toLowerCase()))).limit(5)
              .map(t -> toQualifiedTableName(t))
              .collect(Collectors.toList());
      checkState(notLower.isEmpty(),
              "All returned tables must be lowercase! Found these non-lowercase tables: " + notLower);
    }
    else {
      log.info("showTables is allowing empty table list, actual size is " + tables.size());
    }
    return tables;
  }

  private static GetTableResponse describeTable(TestConfig testConfig, TableName table)
  {
    GetTableResponse tableResponse = LambdaMetadataProvider.getTable(testConfig.getCatalogId(),
                                                                     table,
                                                                     testConfig.getMetadataFunction(),
                                                                     testConfig.getIdentity());
    TableName returnedTableName = tableResponse.getTableName();
    checkState(table.equals(returnedTableName), "Returned table name did not match the requested table name!"
                                                        + " Expected " + toQualifiedTableName(table)
                                                        + " but found " + toQualifiedTableName(returnedTableName));
    List<String> notLower = tableResponse.getSchema().getFields()
                                    .stream()
                                    .map(Field::getName)
                                    .filter(f -> !f.equals(f.toLowerCase()))
                                    .collect(Collectors.toList());
    checkState(notLower.isEmpty(),
                             "All returned columns must be lowercase! Found these non-lowercase columns: " + notLower);
    checkState(tableResponse.getSchema().getFields()
                       .stream().map(Field::getName)
                       .anyMatch(f -> !tableResponse.getPartitionColumns().contains(f)),
                             "Table must have at least one non-partition column!");
    Set<String> fields = tableResponse.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toSet());
    Sets.SetView<String> difference = Sets.difference(tableResponse.getPartitionColumns(), fields);
    checkState(difference.isEmpty(), "Table column list must include all partition columns! "
                                             + "Found these partition columns which are not in the table's fields: "
                                             + difference);
    return tableResponse;
  }

  private static GetTableLayoutResponse getTableLayout(TestConfig testConfig,
                                                       TableName table,
                                                       Schema schema,
                                                       Set<String> partitionColumns)
  {
    Constraints constraints = parseConstraints(schema, testConfig.getConstraints());
    GetTableLayoutResponse tableLayout = LambdaMetadataProvider.getTableLayout(testConfig.getCatalogId(),
                                                        table,
                                                        constraints,
                                                        schema,
                                                        partitionColumns,
                                                        testConfig.getMetadataFunction(),
                                                        testConfig.getIdentity());
    log.info("Found " + tableLayout.getPartitions().getRowCount() + " partitions.");
    checkState(tableLayout.getPartitions().getRowCount() > 0,
                             "Table " + toQualifiedTableName(table)
                       + " did not return any partitions. This can happen if the table"
                       + " is empty but could also indicate an issue."
                       + " Please populate the table or specify a different table.");
    return tableLayout;
  }

  private static GetSplitsResponse getSplits(TestConfig testConfig,
                                             TableName table,
                                             Schema schema,
                                             GetTableLayoutResponse tableLayout,
                                             Set<String> partitionColumns,
                                             String continuationToken)
  {
    Constraints constraints = parseConstraints(schema, testConfig.getConstraints());
    GetSplitsResponse splitsResponse = LambdaMetadataProvider.getSplits(testConfig.getCatalogId(),
                                            table,
                                            constraints,
                                            tableLayout.getPartitions(),
                                            new ArrayList<>(partitionColumns),
                                            continuationToken,
                                            testConfig.getMetadataFunction(),
                                            testConfig.getIdentity());
    log.info("Found " + splitsResponse.getSplits().size() + " splits in batch.");
    if (continuationToken == null) {
      checkState(!splitsResponse.getSplits().isEmpty(),
                               "Table " + toQualifiedTableName(table)
                         + " did not return any splits. This can happen if the table"
                         + " is empty but could also indicate an issue."
                         + " Please populate the table or specify a different table.");
    }
    else {
      checkState(!splitsResponse.getSplits().isEmpty(),
                               "Table " + toQualifiedTableName(table)
                         + " did not return any splits in the second batch despite returning"
                         + " a continuation token with the first batch.");
    }
    return splitsResponse;
  }

  private static ReadRecordsResponse readRecords(TestConfig testConfig,
                                                 TableName table,
                                                 Schema schema,
                                                 Collection<Split> splits)
  {
    Constraints constraints = parseConstraints(schema, testConfig.getConstraints());
    Split split = getRandomElement(splits);
    log.info("Executing randomly selected split with properties: {}", split.getProperties());
    ReadRecordsResponse records = LambdaRecordProvider.readRecords(testConfig.getCatalogId(),
                                                                   table,
                                                                   constraints,
                                                                   schema,
                                                                   split,
                                                                   testConfig.getRecordFunction(),
                                                                   testConfig.getIdentity());
    log.info("Received " + records.getRecordCount() + " records.");
    checkState(records.getRecordCount() > 0,
                             "Table " + toQualifiedTableName(table)
                       + " did not return any rows in the tested split, even though an empty constraint was used."
                       + " This can happen if the table is empty but could also indicate an issue."
                       + " Please populate the table or specify a different table.");
    log.info("Discovered columns: "
                     + records.getSchema().getFields()
                               .stream()
                               .map(f -> f.getName() + ":" + f.getType().getTypeID())
                               .collect(Collectors.toList()));

    if (records.getRecordCount() == 0) {
      return records;
    }

    log.info("First row of split: " + rowToString(records.getRecords(), 0));

    return records;
  }

  private static <T> T getRandomElement(Collection<T> elements)
  {
    int i = RAND.nextInt(elements.size());
    Iterator<T> iter = elements.iterator();
    T elem;
    do {
      elem = iter.next();
      i--;
    } while (i >= 0);
    return elem;
  }

  private static void logTestQuery(String query)
  {
    log.info("==================================================");
    log.info("Testing " + query);
    log.info("==================================================");
  }

  private static void logSuccess()
  {
    log.info("==================================================");
    log.info("Successfully Passed Validation!");
    log.info("==================================================");
  }

  private static void logFailure(Exception ex)
  {
    log.error("==================================================");
    log.error("Error Encountered During Validation!", ex);
    log.error("==================================================");
  }

  private static String toQualifiedTableName(TableName name)
  {
    return name.getSchemaName() + "." + name.getTableName();
  }

  private static class TestConfig
  {
    private static final String LAMBDA_METADATA_FUNCTION_ARG = "lambda-func";
    private static final String LAMBDA_RECORD_FUNCTION_ARG = "record-func";
    private static final String CATALOG_ID_ARG = "catalog";
    private static final String SCHEMA_ID_ARG = "schema";
    private static final String TABLE_ID_ARG = "table";
    private static final String CONSTRAINTS_ARG = "constraints";
    private static final String PLANNING_ONLY_ARG = "planning-only";
    private static final String HELP_ARG = "help";
    private static final String ALLOW_EMPTY_SHOW_TABLES = "allow-empty-show-tables";

    private final FederatedIdentity identity;
    private final String metadataFunction;
    private final String recordFunction;
    private final String catalogId;
    private final Optional<String> schemaId;
    private final Optional<String> tableId;
    private final Optional<String> constraints;
    private final boolean planningOnly;
    private final boolean allowEmptyTables;

    private TestConfig(String metadataFunction,
                       String recordFunction,
                       String catalogId,
                       Optional<String> schemaId,
                       Optional<String> tableId,
                       Optional<String> constraints,
                       boolean planningOnly,
                       boolean allowEmptyTables)
    {
      this.metadataFunction = metadataFunction;
      this.recordFunction = recordFunction;
      this.catalogId = catalogId;
      this.schemaId = schemaId;
      this.tableId = tableId;
      this.constraints = constraints;
      this.planningOnly = planningOnly;
      this.allowEmptyTables = allowEmptyTables;
      this.identity = new FederatedIdentity("VALIDATION_ARN",
                                            "VALIDATION_ACCOUNT",
                                            Collections.emptyMap(),
                                            Collections.emptyList(),
                                            Collections.emptyMap());
    }

    public FederatedIdentity getIdentity()
    {
      return identity;
    }

    String getMetadataFunction()
    {
      return metadataFunction;
    }

    String getRecordFunction()
    {
      return recordFunction;
    }

    String getCatalogId()
    {
      return catalogId;
    }

    Optional<String> getSchemaId()
    {
      return schemaId;
    }

    Optional<String> getTableId()
    {
      return tableId;
    }

    Optional<String> getConstraints()
    {
      return constraints;
    }

    boolean isPlanningOnly()
    {
      return planningOnly;
    }

    boolean isAllowEmptyTables()
    {
      return allowEmptyTables;
    }

    static TestConfig fromArgs(String[] args) throws ParseException
    {
      log.info("Received arguments: {}", args);

      requireNonNull(args);

      Options options = new Options();
      options.addOption("f", LAMBDA_METADATA_FUNCTION_ARG, true,
                                "The name of the Lambda function to be validated. "
                                        + "Uses your configured default AWS region.");
      options.addOption("r", LAMBDA_RECORD_FUNCTION_ARG, true,
                        "The name of the Lambda function to be used to read data records. "
                                + "If not provided, this defaults to the value provided for lambda-func. "
                                + "Uses your configured default AWS region.");
      options.addOption("c", CATALOG_ID_ARG, true,
                        "The catalog name to pass to the Lambda function to be validated.");
      options.addOption("s", SCHEMA_ID_ARG, true,
                        "The schema name to be used when validating the Lambda function. "
                                + "If not provided, a random existing schema will be chosen.");
      options.addOption("t", TABLE_ID_ARG, true,
                        "The table name to be used when validating the Lambda function. "
                                + "If not provided, a random existing table will be chosen.");
      options.addOption("c", CONSTRAINTS_ARG, true,
                        "A comma-separated list of field/value pair constraints to be applied "
                                + "when reading metadata and records from the Lambda function to be validated");
      options.addOption("p", PLANNING_ONLY_ARG, false,
                        "If this option is set, then the validator will not attempt to read"
                                + " any records after calling GetSplits.");
      options.addOption("e", ALLOW_EMPTY_SHOW_TABLES, false,
              "Allows empty show tables results without considering it a failure.");
      options.addOption("h", HELP_ARG, false, "Prints usage information.");
      DefaultParser argParser = new DefaultParser();
      CommandLine parsedArgs = argParser.parse(options, args);

      if (parsedArgs.hasOption(HELP_ARG)) {
        new HelpFormatter().printHelp(150, "./validate_connector.sh --" + LAMBDA_METADATA_FUNCTION_ARG
                                                   + " lambda_func [--" + LAMBDA_RECORD_FUNCTION_ARG
                                                   + " record_func] [--" + CATALOG_ID_ARG
                                                   + " catalog] [--" + SCHEMA_ID_ARG
                                                   + " schema [--" + TABLE_ID_ARG
                                                   + " table [--" + CONSTRAINTS_ARG
                                                   + " constraints]]] [--" + PLANNING_ONLY_ARG + "]"
                                                   + " [--" + ALLOW_EMPTY_SHOW_TABLES + "]"
                                                   + " [--" + HELP_ARG + "]",
                                      null,
                                      options,
                                      null);
        System.exit(0);
      }

      checkArgument(parsedArgs.hasOption(LAMBDA_METADATA_FUNCTION_ARG),
                    "Lambda function must be provided via the --lambda-func or -l args!");
      String metadataFunction = parsedArgs.getOptionValue(LAMBDA_METADATA_FUNCTION_ARG);
      checkArgument(metadataFunction.equals(metadataFunction.toLowerCase()),
                    "Lambda function name must be lowercase.");

      if (parsedArgs.hasOption(TABLE_ID_ARG)) {
        checkArgument(parsedArgs.hasOption(SCHEMA_ID_ARG),
                      "The --schema argument must be provided if the --table argument is provided.");
      }

      if (parsedArgs.hasOption(CONSTRAINTS_ARG)) {
        checkArgument(parsedArgs.hasOption(TABLE_ID_ARG),
                      "The --table argument must be provided if the --constraints argument is provided.");
      }

      String catalog = metadataFunction;
      if (parsedArgs.hasOption(CATALOG_ID_ARG)) {
        catalog = parsedArgs.getOptionValue(CATALOG_ID_ARG);
        checkArgument(catalog.equals(catalog.toLowerCase()),
                      "Catalog name must be lowercase.");
      }

      return new TestConfig(metadataFunction,
                            parsedArgs.hasOption(LAMBDA_RECORD_FUNCTION_ARG)
                                    ? parsedArgs.getOptionValue(LAMBDA_RECORD_FUNCTION_ARG)
                                    : metadataFunction,
                            catalog,
                            Optional.ofNullable(parsedArgs.getOptionValue(SCHEMA_ID_ARG)),
                            Optional.ofNullable(parsedArgs.getOptionValue(TABLE_ID_ARG)),
                            Optional.ofNullable(parsedArgs.getOptionValue(CONSTRAINTS_ARG)),
                            parsedArgs.hasOption(PLANNING_ONLY_ARG),
                            parsedArgs.hasOption(ALLOW_EMPTY_SHOW_TABLES));
    }
  }
}
