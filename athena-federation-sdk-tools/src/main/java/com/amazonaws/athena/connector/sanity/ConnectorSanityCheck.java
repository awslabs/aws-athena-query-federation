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
package com.amazonaws.athena.connector.sanity;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.rowToString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This class should be used to sanity check deployed Lambda functions that use Athena's Federation SDK.
 * It simulates the basic query patterns that Athena will send to Lambda throughout its usage so that
 * broader and sometimes more subtle logical issues can be discovered before being used through Athena.
 * <p>
 * You can run this tool using the following command:
 * mvn exec:java -Dexec.mainClass=com.amazonaws.athena.connector.sanity.ConnectorSanityCheck -Dexec.args="[args]"
 */
public class ConnectorSanityCheck
{
  private static final Logger log = LoggerFactory.getLogger(ConnectorSanityCheck.class);

  private static final Random RAND = new Random();

  private ConnectorSanityCheck()
  {
    // Intentionally left blank.
  }

  /**
   * The main method of this class allows the following argument combinations:
   * lambda-function
   * lambda-function catalog-id
   * lambda-function catalog-id schema-id
   * lambda-function catalog-id schema-id table-id
   * lambda-function catalog-id schema-id table-id record-function
   * <p>
   * The catalog-id, schema-id, and table-id arguments are optional and can be provided in any of the above
   * valid combinations. If any of these are not provided, this test will select one from the returned metadata
   * calls to the Lambda under test. If some of the schemas or tables available through the Lambda are not valid,
   * it is recommended to provide a specific set of identifier arguments to target the test effectively.
   * <p>
   * Note that the last accepted form adds a second Lambda function argument. This is to be used when
   * separate Lambda functions are deployed for metadata and record operations. When this final argument
   * is provided, the first Lambda function argument will represent the metadata function with the latter
   * representing the record function.
   *
   * @param args
   */
  public static void main(String[] args)
  {
    try {
      TestConfig testConfig = TestConfig.fromArgs(args);

      // SHOW DATABASES
      logTestQuery("SHOW DATABASES");
      Collection<String> schemas = showDatabases(testConfig);

      // SHOW TABLES
      final String db = testConfig.getSchemaId().isPresent()
                                ? testConfig.getSchemaId().get()
                                : getRandomElement(schemas);
      log.info("Using database {}", db);
      logTestQuery("SHOW TABLES IN " + db);
      Collection<TableName> tables = showTables(testConfig, db);

      // DESCRIBE TABLE
      final TableName table = testConfig.getTableId().isPresent()
                                      ? new TableName(db, testConfig.getTableId().get())
                                      : getRandomElement(tables);
      log.info("Using table {}", toQualifiedTableName(table));
      logTestQuery("DESCRIBE " + toQualifiedTableName(table));
      GetTableResponse tableResponse = describeTable(testConfig, table);
      final Schema schema = tableResponse.getSchema();
      final Set<String> partitionColumns = tableResponse.getPartitionColumns();

      // SELECT
      logTestQuery("SELECT * FROM " + toQualifiedTableName(table));
      GetTableLayoutResponse tableLayout = getTableLayout(testConfig, table, schema, partitionColumns);

      GetSplitsResponse splitsResponse = getSplits(testConfig, table, tableLayout, partitionColumns, null);
      readRecords(testConfig, table, schema, splitsResponse.getSplits());

      if (splitsResponse.getContinuationToken() == null) {
        logSuccess();
        return;
      }

      log.info("GetSplits included a continuation token: " + splitsResponse.getContinuationToken());
      log.info("Testing next batch of splits.");

      splitsResponse = getSplits(testConfig, table, tableLayout, partitionColumns, splitsResponse.getContinuationToken());
      readRecords(testConfig, table, schema, splitsResponse.getSplits());

      logSuccess();
    }
    catch (Exception ex) {
      logFailure(ex);
    }
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
    checkState(!tables.isEmpty(), "No tables were returned!");
    List<String> notLower = tables.stream().filter(t -> !t.equals(new TableName(t.getSchemaName().toLowerCase(),
                                                                                t.getTableName().toLowerCase()))).limit(5)
                                    .map(t -> toQualifiedTableName(t))
                                    .collect(Collectors.toList());
    checkState(notLower.isEmpty(),
                             "All returned tables must be lowercase! Found these non-lowercase tables: " + notLower);
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
    Map<String, ValueSet> summary = new HashMap<>();
    Constraints constraints = new Constraints(summary);
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
                                             GetTableLayoutResponse tableLayout,
                                             Set<String> partitionColumns,
                                             String continuationToken)
  {
    Map<String, ValueSet> summary = new HashMap<>();
    Constraints constraints = new Constraints(summary);
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
    Map<String, ValueSet> summary = new HashMap<>();
    Constraints constraints = new Constraints(summary);
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
    log.info("Successfully Passed Sanity Test!");
    log.info("==================================================");
  }

  private static void logFailure(Exception ex)
  {
    log.error("==================================================");
    log.error("Error Encountered During Sanity Test!", ex);
    log.error("==================================================");
  }

  private static String toQualifiedTableName(TableName name)
  {
    return name.getSchemaName() + "." + name.getTableName();
  }

  private static class TestConfig
  {
    private static final int LAMBDA_METADATA_FUNCTION_ARG = 0;
    private static final int CATALOG_ID_ARG = 1;
    private static final int SCHEMA_ID_ARG = 2;
    private static final int TABLE_ID_ARG = 3;
    private static final int LAMBDA_RECORD_FUNCTION_ARG = 4;

    private final FederatedIdentity identity;
    private final String metadataFunction;
    private final String recordFunction;
    private final String catalogId;
    private final Optional<String> schemaId;
    private final Optional<String> tableId;

    private TestConfig(String metadataFunction,
                       String recordFunction,
                       String catalogId,
                       Optional<String> schemaId,
                       Optional<String> tableId)
    {
      this.metadataFunction = metadataFunction;
      this.recordFunction = recordFunction;
      this.catalogId = catalogId;
      this.schemaId = schemaId;
      this.tableId = tableId;
      this.identity = new FederatedIdentity("SANITY_ACCESS_KEY",
                                            "SANITY_PRINCIPAL",
                                            "SANITY_ACCOUNT");
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

    static TestConfig fromArgs(String[] args)
    {
      requireNonNull(args);

      Optional<String> metadataFunction = getArgument(args, LAMBDA_METADATA_FUNCTION_ARG);
      Optional<String> catalogId = getArgument(args, CATALOG_ID_ARG);
      Optional<String> schemaId = getArgument(args, SCHEMA_ID_ARG);
      Optional<String> tableId = getArgument(args, TABLE_ID_ARG);
      Optional<String> recordFunction = getArgument(args, LAMBDA_RECORD_FUNCTION_ARG);

      checkArgument(metadataFunction.isPresent(), "The Function argument must be provided.");

      return new TestConfig(metadataFunction.get(),
                            recordFunction.isPresent() ? recordFunction.get() : metadataFunction.get(),
                            catalogId.isPresent() ? catalogId.get() : metadataFunction.get(),
                            schemaId,
                            tableId);
    }

    static Optional<String> getArgument(String[] args, int index)
    {
      if (args.length > index && args[index] != null && args[index].length() > 0) {
        return Optional.of(args[index]);
      }
      return Optional.empty();
    }
  }
}
