/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse.integ;

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
 * Integration-tests for the Synapse (JDBC) connector using the Integration-test module.
 */
public class AzureSynapseIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AzureSynapseIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;

    private static  TestConfig testConfig;
    private static  String schemaName;
    private static  String tableName;
    private static String tableName1;
    private static String tableName2;
    private static  String tableName5;
    private static  String tableName6;
    private static  String tableName7;
    private String lambdaAndSchemaName;

    static {
        testConfig = new TestConfig();
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName = testConfig.getStringItem("table_name").get();
        tableName1 = testConfig.getStringItem("table_name1").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        tableName5 = testConfig.getStringItem("table_name5").get();
        tableName6 = testConfig.getStringItem("table_name6").get();
        tableName7 = testConfig.getStringItem("table_name7").get();

        System.setProperty("aws.region", testConfig.getStringItem("region").get());
        System.setProperty("aws.accessKeyId", testConfig.getStringItem("access_key").get());
        System.setProperty("aws.secretKey", testConfig.getStringItem("secret_key").get());
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
        // no-op
    }

    @Override
    protected void setUpTableData() {
        // no-op
    }

    @Override
    protected void setUpStackData(Stack stack) {
        // no-op
    }

    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        super.setUp();
    }

    public AzureSynapseIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndSchemaName = "\"lambda:"+lambdaFunctionName+"\".DBO.";
    }

    @Test
    public void getTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(schemaName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(schemaName,tableName,lambdaFunctionName);
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(schemaName,tableName,lambdaFunctionName,"name","Confirmed");
        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(schemaName,tableName,lambdaFunctionName,"name","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupBy(schemaName,tableName,lambdaFunctionName,"name");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupByHavingClause(schemaName,tableName,lambdaFunctionName,"name","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void useUnion()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List tableNames = fetchDataUnion(schemaName,tableName,tableName1,lambdaFunctionName,lambdaFunctionName,"name","Confirmed");
        logger.info("Tables: {}", tableNames);
    }

    @Test
    public void checkDataTypes() {
        logger.info("-----------------------------------");
        logger.info("Executing checkDataTypes");
        logger.info("-----------------------------------");
        List datetimeColValues = processQuery("SELECT datetimecol FROM " + lambdaAndSchemaName + tableName5 + " where datetimecol <= TIMESTAMP  '2021-07-12 12:10:05.137'");
        logger.info("datetimeColValues : {}", datetimeColValues);
        List datetime2ColValues  = processQuery("SELECT datetime2col FROM " + lambdaAndSchemaName + tableName5 + " where datetime2col = TIMESTAMP '2021-07-14 12:22:05.140'");
        logger.info("datetime2ColValues : {}", datetime2ColValues);
        List smalldatetimeColValues = processQuery("SELECT smalldatetimecol FROM " + lambdaAndSchemaName + tableName5 + " where smalldatetimecol = TIMESTAMP '1955-12-13 12:43:00'");
        logger.info("smalldatetimeColValues : {}", smalldatetimeColValues);
        List dateColValues = processQuery("SELECT datecol FROM " + lambdaAndSchemaName + tableName5 + " where datecol = DATE '1912-10-25'");
        logger.info("dateColValues : {}", dateColValues);
        List timeColValues = processQuery("SELECT timecol FROM " + lambdaAndSchemaName + tableName5 + " where timecol = '12:34:54.1237000'");
        logger.info("timeColValues : {}", timeColValues);
        List datetimeoffsetColValues = processQuery("SELECT datetimeoffsetcol FROM " + lambdaAndSchemaName + tableName5 + " where datetimeoffsetcol <= TIMESTAMP '2012-10-25 12:32:10.000'");
        logger.info("datetimeoffsetColValues : {}", datetimeoffsetColValues);
        List floatColValues = processQuery("SELECT floatcol24 FROM " + lambdaAndSchemaName + tableName6 + " where floatcol24 >= -45877458555845454.575");
        logger.info("floatColValues : {}", floatColValues);
        List useAggrSum = processQuery("select sum(tinyintcol) from " + lambdaAndSchemaName + tableName7);
        logger.info("use aggrgate function SUM : {}", useAggrSum);
    }

    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, tableName2);
        logger.info("throughput for Synapse connector: "+ throughput);
    }

}
