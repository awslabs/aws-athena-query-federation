/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana.integ;

import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

//import static org.junit.Assert.assertEquals;

/**
 * Integration-tests for the MySql (JDBC) connector using the Integration-test module.
 */
public class SaphanaIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SaphanaIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;
    private static  TestConfig testConfig;
    private static final String REGION ="region";
    private static  String saphanaDBName;
    private static final String SAPHANA_DB_NAME ="saphana_db_name";
    private static final String TABLE_NAME ="table_name";
    private static final String TABLE_NAME1 ="table_name1";
    private static final String HASHTABLE_NAME ="table_name";
    private static final String RANGETABLE_NAME ="table_name1";
    private static final String ROUNDROBINTABLE_NAME ="table_name";
    private static  String tableName;
    private static String tableName1;
    private static String hashPartitionTable;
    private static String rangePartitionTable;
    private static String roundRobinPartitionTable;

    private static  String region;
    static {
        testConfig = new TestConfig();
        region = testConfig.getStringItem(REGION).get();
        saphanaDBName = testConfig.getStringItem(SAPHANA_DB_NAME).get();
        tableName = testConfig.getStringItem(TABLE_NAME).get();
        tableName1 = testConfig.getStringItem(TABLE_NAME1).get();
        hashPartitionTable = testConfig.getStringItem(HASHTABLE_NAME).get();
        rangePartitionTable = testConfig.getStringItem(RANGETABLE_NAME).get();
        roundRobinPartitionTable = testConfig.getStringItem(ROUNDROBINTABLE_NAME).get();
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

    public SaphanaIntegTest()
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
        List tableNames = fetchDataSelect(saphanaDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataTypes()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataTypes");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE SALARY=11000";
        logger.info("Executing fetchDataSelectCountAllIntegTest");
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE AGE='21'";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE BONUS='13245678'";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE NAME='ANYA'";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE WORKHOUR='12:17:20'";
        processQuery( queryTableCreation);

        // Decimal value in where clause
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE LEAVE=1.9";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "." + "EMP" + "\n" +
                " WHERE LEAVE=8.0";
        processQuery( queryTableCreation);
    }


    @Test
    public void fetchDataSelectCountAllIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectCountAllIntegTest");
        logger.info("-----------------------------------");

        String queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ saphanaDBName + "." + tableName + "\n" +
                " limit 10";
        logger.info("Executing fetchDataSelectCountAllIntegTest");
        processQuery( queryTableCreation);
    }

    @Test
    public void fetchDataWhereClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(saphanaDBName,tableName,lambdaFunctionName,"case_type","Confirmed");

        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataWhereClauseLIKEIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseLIKEIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(saphanaDBName,tableName,lambdaFunctionName,"case_type","Confirmed");

        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupBy(saphanaDBName,tableName,lambdaFunctionName,"case_type");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByHavingClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByHavingClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupByHavingClause(saphanaDBName,tableName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataUnionTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataUnionTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataUnion(saphanaDBName,tableName,tableName1,lambdaFunctionName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseDistinctIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseDistinctIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataDistinct(lambdaFunctionName,saphanaDBName,tableName,"case_type","case_type","'Confirmed'");

        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataSelectIntegTesthashPartitionTable()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(saphanaDBName,hashPartitionTable,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataSelectIntegTestroundRobinPartitionTable()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectIntegTesthashPartitionTable");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(saphanaDBName,roundRobinPartitionTable,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataSelectIntegTestrangePartitionTable()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectIntegTestrangePartitionTable");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(saphanaDBName,rangePartitionTable,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, saphanaDBName , "covid19_bulkdata");
        logger.info("throughput for SAPHANA connector: "+ throughput);
    }

    /**
     *
     * @param schemaName
     * @param tableName
     * @param lambdaFnName
     */
    public float calculateThroughput(String lambdaFnName, String schemaName,String tableName)
    {
        logger.info("Executing calculateThroughput");
        logger.info("Connector Lambda Name:" + lambdaFnName);
        logger.info("Schema Name :" + schemaName);
        logger.info("Table Name :" + tableName);

        // running the query to get total no of records
        List<String> list = fetchDataSelectCountAll(schemaName, tableName, lambdaFnName);
        long numberOfRecords = Long.parseLong( list.get(0).toString());

        long startTimeInMillis = System.currentTimeMillis();
        fetchDataSelect(schemaName, tableName, lambdaFnName);
        long endTimeInMillis = System.currentTimeMillis();
        float elapsedSeconds = (float) (endTimeInMillis - startTimeInMillis) /1000F;
        logger.info("Total Record count:" + numberOfRecords);
        logger.info("Total time taken in seconds : " + elapsedSeconds);
        float throughput = numberOfRecords/elapsedSeconds;
        logger.info("Total throughput(Records per Second) :" + throughput);
        return throughput;
    }
    @Test
    public void fetchDataSelectViewIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectViewIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(saphanaDBName,"VIEWCOVID119",lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void testMultiplePartitionsBySelectingFromLargeTable()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testMultiplePartitionsBySelectingFromLargeTable");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "."
                + "COVID19_RR" +
                " WHERE COMBINED_KEY='Gregg, Texas, US' LIMIT 10";
        logger.info("Executing testMultiplePartitionsBySelectingFromLargeTable");
        processQuery( queryTableCreation);
        logger.info("Executing ended testMultiplePartitionsBySelectingFromLargeTable");
    }

    @Test
    public void testDataType()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testDataType");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "SYSTEM" + "."
                + "T1";
        logger.info("Executing testDataType");
        processQuery( queryTableCreation);
        logger.info("Executing ended testDataType");
    }

    @Test
    public void testGeoSpatialDataTypeFromOnPremInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testGeoSpatialDataTypeFromOnPremInstance");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "ATHENA" + "."
                + "CUSTOMER_ADDRESSES LIMIT 10";
        logger.info("Executing testGeoSpatialDataTypeFromOnPremInstance");
        processQuery( queryTableCreation);
        logger.info("Executing ended testGeoSpatialDataTypeFromOnPremInstance");
    }

    @Test
    public void testMostVarcharColsWithFewSpatialColsOnPremInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testGeoSpatialDataTypeFromOnPremInstance");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "ATHENA" + "."
                + "test_varchar_stpoint LIMIT 10";
        logger.info("Executing testAddNewColumnErrorOnPremInstance");
        processQuery( queryTableCreation);
        logger.info("Executing ended testAddNewColumnErrorOnPremInstance");
    }

    @Test
    public void testMostSpatialColumnsOnPremInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testGeoSpatialDataTypeFromOnPremInstance");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "ATHENA" + "."
                + "test_stpoint LIMIT 10";
        logger.info("Executing testAddNewColumnErrorOnPremInstance");
        processQuery( queryTableCreation);
        logger.info("Executing ended testAddNewColumnErrorOnPremInstance");
    }

    @Test
    public void testDAYDATEFieldTypeOnPremInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testDAYDATEFieldTypeOnPremInstance");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select BIRTHDT, HOME_ADDRESS from \"lambda:" + lambdaFunctionName
                + "\"."+ "ATHENA" + "." + "BUT001 LIMIT 10";
        logger.info("Executing testDAYDATEFieldTypeOnPremInstance");
        processQuery( queryTableCreation);
        logger.info("Executing ended testDAYDATEFieldTypeOnPremInstance");
    }

    @Test
    public void testAllSpatialSubTypesOnPremInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testAllSpatialSubTypesOnPremInstance");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName
                + "\"."+ "ATHENA" + "." + "SPATIAL_DATA LIMIT 10";
        logger.info("Executing testAllSpatialSubTypesOnPremInstance");
        processQuery( queryTableCreation);
        logger.info("Executing ended testAllSpatialSubTypesOnPremInstance");
    }

    /**
     * The following tests are only intended to test the cloud SAP HANA instance
     *
     * Please note the following:
     * Currently we have 2 test-config.json (finally config file name should look like exactly as 'test-config.json'):
     * 1. test-config-on-prep.json
     * 2. test-config-cloud.json
     *
     * Please rename test-config.json to test-config.json AND test-config-cloud.json to test-config.json
     * Uncomment the @Ignore annotation
     *
     */
    @Test
    @Ignore
    public void testMultiplePartitionsBySelectingFromLargeTableFromCloudInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testMultiplePartitionsBySelectingFromLargeTableFromCloudInstance");
        logger.info("-----------------------------------");

        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "DBADMIN" + "."
                + "CUSTOMER LIMIT 10";
        logger.info("Executing testMultiplePartitionsBySelectingFromLargeTableFromCloudInstance");
        processQuery( queryTableCreation);
        logger.info("Executing ended testMultiplePartitionsBySelectingFromLargeTableFromCloudInstance");
    }

    @Test
    @Ignore
    public void testGeoSpatialDataTypeFromCloudInstance()
    {
        logger.info("-----------------------------------");
        logger.info("Executing testGeoSpatialDataTypeFromCloudInstance");
        logger.info("-----------------------------------");

        String  queryCustomerAddress = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "DBADMIN" + "."
                + "CUSTOMER_ADDRESSES LIMIT 10";

        logger.info("Executing testGeoSpatialDataTypeFromCloudInstance");
        processQuery( queryCustomerAddress);
        logger.info("Executing ended testGeoSpatialDataTypeFromCloudInstance");
    }
}
