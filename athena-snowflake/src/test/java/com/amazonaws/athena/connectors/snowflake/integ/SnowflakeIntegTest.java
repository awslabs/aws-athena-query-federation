/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

//import static org.junit.Assert.assertEquals;

/**
 * Integration-tests for the Snowflake (JDBC) connector using the Integration-test module.
 */
public class SnowflakeIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;
    private static  TestConfig testConfig;
    private static final String REGION ="region";
    private static  String snowflakeDBName;
    private static final String SNOWFLAKE_DB_NAME ="snowflake_db_name";
    private static final String TABLE_NAME ="table_name";
    private static final String TABLE_NAME1 ="table_name1";
    private static final String VIEW_NAME = "view_name";
    private static  String tableName;
    private static String tableName1;
    private static String viewName;

    private static  String region;
    static {
        testConfig = new TestConfig();
        region = testConfig.getStringItem(REGION).get();
        snowflakeDBName = testConfig.getStringItem(SNOWFLAKE_DB_NAME).get();
        tableName = testConfig.getStringItem(TABLE_NAME).get();
        tableName1 = testConfig.getStringItem(TABLE_NAME1).get();
        viewName = testConfig.getStringItem(VIEW_NAME).get();
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

    public SnowflakeIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();

    }

    @Test
    public void fetchDataTypes()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataTypes");
        logger.info("-----------------------------------");
        // Test view files
        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "VIEWFOREMPLOYEE";
        processQuery( queryTableCreation);
        /**
         * timestamp test
         */
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "TS_TEST2 WHERE TS_NTZ>=TIMESTAMP '2016-10-19 15:39:47.000' ";
        processQuery( queryTableCreation);
        /**
         * limit test
         */
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "AAB_COMPLETEDCARD" +
                " limit 2 ";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "AAB_COMPLETEDACCT" +
                " where PARSEDDATE=date('2013-01-01')";
        processQuery( queryTableCreation);


        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "AAB_COMPLETEDACCT" +
                " where district_id=58 ";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "AAB_COMPLETEDCARD" +
                " where type='credit'";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "EMPLOYEE" +
                " where floatcol=4.0";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "EMPLOYEE" +
                " where decimalcol=6.829";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "EMPLOYEE" +
                " where doublecol=23.983";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "EMPLOYEE" +
                " where smallintcol=234";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "EMPLOYEE" +
                " where bigintcol=123332";
        processQuery( queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "CMRN" + "." + "var";
        processQuery( queryTableCreation);

    }

    @Test
    public void fetchDataSelectIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(snowflakeDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataSelectCountAllIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(snowflakeDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesI" +
                "ntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(snowflakeDBName,tableName1,lambdaFunctionName,"FREQUENCY","Monthly Issuance");

        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataWhereClauseLIKEIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing listTablesIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(snowflakeDBName,tableName1,lambdaFunctionName,"FREQUENCY","Issuance");

        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupBy(snowflakeDBName,tableName1,lambdaFunctionName,"FREQUENCY");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByHavingClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByHavingClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupByHavingClause(snowflakeDBName,tableName1,lambdaFunctionName,"FREQUENCY","Issuance");
        logger.info("Tables: {}", tableNames);
    }

   @Test
    public void fetchDataUnionTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataUnionTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataUnion(snowflakeDBName,tableName1,tableName1,lambdaFunctionName,lambdaFunctionName,"FREQUENCY","Issuance");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseDistinctIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseDistinctIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataDistinct(lambdaFunctionName,snowflakeDBName,tableName1,"FREQUENCY","FREQUENCY","'Issuance'");

        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataSelectIntegViewTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseDistinctIntegTest");
        logger.info("-----------------------------------");
        List tableNames =fetchDataSelect(snowflakeDBName,viewName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void runPartionTest() {
        /**
         *  Table: AAB_COMPLETEDCARD
         *  Total Records : 3
         *  pagecount: 1000
         *  partitionlimit: 5
         *
         *  Output:
         *  No partition
         *
         *  Return SQL: SELECT "CARD_ID", "DISP_ID", "TYPE", "YEAR", "MONTH", "DAY", "FULLDATE"
         *  FROM "CMRN"."AAB_COMPLETEDCARD"  limit 1000 offset 0
         */
        fetchDataSelect("CMRN", "AAB_COMPLETEDCARD", lambdaFunctionName);


        /**
         *  Table: AAB_COMPLETEDACCT
         *  Total Records : 4500
         *
         *  Output:
         *  5 partition
         *
         *
         *
         *  SQL1: SELECT "ACCOUNT_ID", "DISTRICT_ID", "FREQUENCY", "PARSEDDATE", "YEAR", "MONTH", "DAY" FROM "CMRN"."AAB_COMPLETEDACCT"
         *  limit 1000 offset 0
         *
         *  SELECT "ACCOUNT_ID", "DISTRICT_ID", "FREQUENCY", "PARSEDDATE", "YEAR", "MONTH", "DAY" FROM "CMRN"."AAB_COMPLETEDACCT"
         *  limit 1000 offset 1000
         *
         *  SELECT "ACCOUNT_ID", "DISTRICT_ID", "FREQUENCY", "PARSEDDATE", "YEAR", "MONTH", "DAY" FROM "CMRN"."AAB_COMPLETEDACCT"
         *          limit 1000 offset 2000
         *  SELECT "ACCOUNT_ID", "DISTRICT_ID", "FREQUENCY", "PARSEDDATE", "YEAR", "MONTH", "DAY" FROM "CMRN"."AAB_COMPLETEDACCT"
         *  limit 1000 offset 3000
         *
         * SELECT "ACCOUNT_ID", "DISTRICT_ID", "FREQUENCY", "PARSEDDATE", "YEAR", "MONTH", "DAY" FROM "CMRN"."AAB_COMPLETEDACCT"
         * limit 1000 offset 4000
         *
         */
        fetchDataSelect("CMRN", "AAB_COMPLETEDACCT", lambdaFunctionName);

        /**
         *  Table: AAB_COMPLETEDACCT
         *  Total Records : 12942
         *   pagecount: 1000
         *   partitionlimit: 5
         *
         *  Output:
         *
         *  Total records 12,942
         *  Total Limit = 12,942/1000 = 13
         *  As the limit exceeds the total capacity 5 so no partition
         *
         */
        fetchDataSelect("CMRN", "AAB_COMPLETEDORDER", lambdaFunctionName);
    }



    @Test
    public void getLargeTableWithLimit() {
       String queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "ATHENA" + "." + "COVID19_DATA_100M" +
                " limit 100000 ";
        processQuery( queryTableCreation);
    }


    /**
     * limit test
     */



    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, "ATHENA" , "COVID19_DATA");
        logger.info("throughput for SnowFlake connector: "+ throughput);
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

        //running the query to get total no of records
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
}
