/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata.integ;

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


/**
 * Integration-tests for the Teradata (JDBC) connector using the Integration-test module.
 */
public class TeradataIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TeradataIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;
    private static  TestConfig testConfig;
    private static final String REGION ="region";
    private static  String teradataDBName;
    private static final String TERADATA_DB_NAME ="teradata_db_name";
    private static final String TABLE_NAME ="table_name";
    private static final String TABLE_NAME1 ="table_name1";
    private static  String tableName;
    private static String tableName1;

    private static  String region;
    static {
        testConfig = new TestConfig();
        region = testConfig.getStringItem(REGION).get();
        teradataDBName = testConfig.getStringItem(TERADATA_DB_NAME).get();
        tableName = testConfig.getStringItem(TABLE_NAME).get();
        tableName1 = testConfig.getStringItem(TABLE_NAME1).get();
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

    public TeradataIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();

    }
    @Test
    public void fetchDataTypes() {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataTypes");
        logger.info("-----------------------------------");

        String queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "datatype3_check" +
                " where intcol=1.0";
        processQuery(queryTableCreation);
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where  id=1";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where name='Ankita'";
        processQuery(queryTableCreation);
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where val3<=9.9";
        processQuery(queryTableCreation);
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "EMPL1" +
                " where col3>=4.5555553";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where column6 = '2019-02-15 06:22:11.871'";
        processQuery(queryTableCreation);


        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where column7 = '15:18:07'";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "EMPLOYEE" +
                " where hire_date =date('2020-11-11')";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where val2 =1601111111111119111";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where val5=1";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where column12 = 126";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where VAL4=2.0";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where VAL6 = 'A' ";
        processQuery(queryTableCreation);

        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "view_sample";
        processQuery(queryTableCreation);


        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "datatype3_check" +
                " where intcol=1";
        processQuery(queryTableCreation);


        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"." + "test" + "." + "T5" +
                " where VAL4=2";
        processQuery(queryTableCreation);


    }

    @Test
    public void fetchDataSelectIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(teradataDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataSelectCountAllIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataSelectCountAllIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(teradataDBName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(teradataDBName,tableName,lambdaFunctionName,"case_type","Confirmed");

        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseDistinctIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseDistinctIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataDistinct(lambdaFunctionName,teradataDBName,tableName,"case_type","case_type","'Confirmed'");

        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataWhereClauseLIKEIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseLIKEIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(teradataDBName,tableName,lambdaFunctionName,"case_type","Confirmed");

        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupBy(teradataDBName,tableName,lambdaFunctionName,"case_type");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataGroupByHavingClauseIntegTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataGroupByHavingClauseIntegTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupByHavingClause(teradataDBName,tableName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void fetchDataUnionTest()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataUnionTest");
        logger.info("-----------------------------------");
        List tableNames = fetchDataUnion(teradataDBName,tableName,tableName1,lambdaFunctionName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, teradataDBName, "covid19_cp");
        logger.info("throughput for teradata connector: "+ throughput);
    }

    /**
     *
     * @param schemaName
     * @param tablename
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

}
