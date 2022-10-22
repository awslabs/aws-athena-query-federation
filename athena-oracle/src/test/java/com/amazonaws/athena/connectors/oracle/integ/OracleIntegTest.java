/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.oracle.integ;

import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.amazonaws.services.athena.model.Row;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import static org.junit.Assert.assertEquals;

/**
 * Integration-tests for the ORACLE (JDBC) connector using the Integration-test module.
 */
public class OracleIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(OracleIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;
    private static  TestConfig testConfig;
    private static final String ACCESS_KEY ="access_key";
    private static final String SECRET_KEY ="secret_key";
    private static final String REGION ="region";
    private static final String ORACLE_DB_NAME ="oracle_db_name";
    private static final String TABLE_NAME ="table_name";
    private static final String TABLE_NAME1 ="table_name1";
    private static final String RANGE_PARTITION_TABLE ="rangePartitionTable";
    private static final String ALL_DATA_TYPE_TABLE ="allDataTypeTable";
    private static final String LIST_PARTITION_TABLE ="listPartitionTable";
    private static  String region;
    private static  String oracleDBName;
    private static  String tableName;
    private static String tableName1;

    private static  String rangePartitionTable;
    private static  String allDataTypeTable;
    private static String listPartitionTable;
    static {
        testConfig = new TestConfig();
        region = testConfig.getStringItem(REGION).get();
        oracleDBName = testConfig.getStringItem(ORACLE_DB_NAME).get();
        tableName = testConfig.getStringItem(TABLE_NAME).get();
        tableName1 = testConfig.getStringItem(TABLE_NAME1).get();
        rangePartitionTable = testConfig.getStringItem(RANGE_PARTITION_TABLE).get();
        allDataTypeTable = testConfig.getStringItem(ALL_DATA_TYPE_TABLE).get();
        listPartitionTable = testConfig.getStringItem(LIST_PARTITION_TABLE).get();
    }
    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        List<String> statementActionsPolicy = new ArrayList<>();
        statementActionsPolicy.add("*");
        return Optional.of(PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(statementActionsPolicy)
                        .resources(ImmutableList.of("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build());
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        // environmentVars.putAll(this.environmentVars);
    }

    @Override
    protected void setUpTableData() {

    }

    /**
     * Sets up connector-specific Cloud Formation resource.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        // No-op.
    }

    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        super.setUp();
    }

    public OracleIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();

    }

    @Test
    public void fetchDataSelectIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(oracleDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataSelectCountAllIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectCountAllIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(oracleDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataSelectCountAllIntegTestPartition()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectCountAllIntegTestPartition");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(oracleDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(oracleDBName,tableName,lambdaFunctionName,"case_type","Confirmed");

        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataWhereClauseLIKEIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseLIKEIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(oracleDBName,tableName,lambdaFunctionName,"case_type","Confirmed");

        logger.info("Tables: {}", tableNames);
    }


    @Test
    public void fetchDataGroupByIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupBy(oracleDBName,tableName,lambdaFunctionName,"case_type");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByHavingClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByHavingClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupByHavingClause(oracleDBName,tableName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataUnionTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataUnionTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataUnion(oracleDBName,tableName,tableName1,lambdaFunctionName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchRangePartitionDataTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing selectColumnWithPredicateIntegTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\";", lambdaFunctionName, oracleDBName ,rangePartitionTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> rangePartitonData = new ArrayList<>();
        rows.forEach(row -> rangePartitonData.add(row.getData().get(0).getVarCharValue()));
        logger.info("rangePartitonData: {}", rangePartitonData);
    }

    @Test
    public void fetchAllDataTypeDataTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing fetchAllDataTypeDataTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\";", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> allDataTypeData = new ArrayList<>();
        rows.forEach(row -> allDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("allDataTypeData: {}", allDataTypeData);
    }

    @Test
    public void fetchListPartitionDataTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing fetchListPartitionDataTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\";", lambdaFunctionName, oracleDBName ,listPartitionTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> listPartitonData = new ArrayList<>();
        rows.forEach(row -> listPartitonData.add(row.getData().get(0).getVarCharValue()));
        logger.info("listPartitonData: {}", listPartitonData);
    }

    @Test
    public void numberDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing numberDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where NUMBER_TYPE=320;", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> numberDataTypeData = new ArrayList<>();
        rows.forEach(row -> numberDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("numberDataTypeData: {}", numberDataTypeData);
    }

    @Test
    public void charDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing charDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where CHAR_TYPE='A';", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> charDataTypeData = new ArrayList<>();
        rows.forEach(row -> charDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("charDataTypeData: {}", charDataTypeData);
    }

    @Test
    public void dateDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing dateDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where DATE_COL = date('2021-03-18');", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> dateDataTypeData = new ArrayList<>();
        rows.forEach(row -> dateDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("dateDataTypeData: {}", dateDataTypeData);
    }

    @Test
    public void timestampDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing timestampDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where TIMESTAMP_WITH_3_FRAC_SEC_COL >= CAST('2021-03-18 09:00:00.123' AS TIMESTAMP);", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> timestampDataTypeData = new ArrayList<>();
        rows.forEach(row -> timestampDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("timestampDataTypeData: {}", timestampDataTypeData);
    }

    @Test
    public void varcharDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing varcharDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where VARCHAR_10_COL ='ORACLEXPR';", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> varcharDataTypeData = new ArrayList<>();
        rows.forEach(row -> varcharDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("varcharDataTypeData: {}", varcharDataTypeData);
    }

    @Test
    public void decimalDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing decimalDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where NUMBER_3_SF_2_DP = 5.82;", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> decimalDataTypeData = new ArrayList<>();
        rows.forEach(row -> decimalDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("decimalDataTypeData: {}", decimalDataTypeData);
    }

    @Test
    public void multiDataTypefilterClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing multiDataTypefilterClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where DATE_COL= date('2021-03-18') and NUMBER_3_SF_2_DP = 5.82;", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> multiDataTypeFilterData = new ArrayList<>();
        rows.forEach(row -> multiDataTypeFilterData.add(row.getData().get(0).getVarCharValue()));
        logger.info("multiDataTypeFilterData: {}", multiDataTypeFilterData);
    }


    @Test
    public void floatDataTypeWhereClauseTest()
    {
        logger.info("--------------------------------------------------");
        logger.info("Executing floatDataTypeWhereClauseTest");
        logger.info("--------------------------------------------------");

        String query = String.format("select * from   \"lambda:%s\".\"%s\".\"%s\" where float_col = 39840.0;", lambdaFunctionName, oracleDBName ,allDataTypeTable);
        List<Row> rows = startQueryExecution(query).getResultSet().getRows();
        if (!rows.isEmpty()) {
            // Remove the column-header row
            rows.remove(0);
        }
        List<String> floatDataTypeData = new ArrayList<>();
        rows.forEach(row -> floatDataTypeData.add(row.getData().get(0).getVarCharValue()));
        logger.info("floatDataTypeData: {}", floatDataTypeData);
    }

    /**
     * Checking performance of the connector
     */
    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, oracleDBName, "COVID19_1");
        logger.info("throughput for oracle connector: "+ throughput);
    }

}
