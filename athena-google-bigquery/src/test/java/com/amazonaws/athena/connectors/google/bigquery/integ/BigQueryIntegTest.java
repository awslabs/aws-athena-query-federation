/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.amazonaws.services.athena.model.Row;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BigQueryIntegTest  extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;
    private static final String TABLE_NAME ="table_name";

    private static TestConfig testConfig;
    private static String schemaName;
    private static String tableName2;
    private static String tableName3;
    private static String tableName4;
    private static String tableName5;
    private static String lambdaAndSchemaName;
    private static  String tableName;


    static {
        testConfig = new TestConfig();
        tableName = testConfig.getStringItem(TABLE_NAME).get();
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        tableName3 = testConfig.getStringItem("table_name3").get();
        tableName4 = testConfig.getStringItem("table_name4").get();
        tableName5 = testConfig.getStringItem("table_name5").get();
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy() {

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
    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public BigQueryIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndSchemaName = "\"lambda:"+lambdaFunctionName+"\"."+schemaName+".";
    }

    @Test
    public void getTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataSelect(schemaName,tableName4,lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableCount = fetchDataSelectCountAll(schemaName,tableName4,lambdaFunctionName);
        logger.info("table count: {}", tableCount.get(0));
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClause(schemaName,tableName4,lambdaFunctionName,"case_type","Death");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }
    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClauseLIKE(schemaName,tableName4,lambdaFunctionName,"case_type","%Death%");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupBy(schemaName,tableName2,lambdaFunctionName, "case_type");
        logger.info("count(case_type) values: {}", firstColValues);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupByHavingClause(schemaName,tableName2,lambdaFunctionName,"case_type","Confirmed");
        logger.info("count(case_type) Having 'Confirmed' values : {}", firstColValues);
    }

    @Test
    public void useUnion()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataUnion(schemaName,tableName2,tableName2,lambdaFunctionName,lambdaFunctionName,"case_type","Confirmed");
        logger.info("union where case_type 'Confirmed' values : {}", firstColValues);
    }

    @Test
    public void getPartitionTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getPartitionTableData");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataSelect(schemaName,tableName3,lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void checkDataTypes() {
        logger.info("-----------------------------------");
        logger.info("Executing checkDataTypes");
        logger.info("-----------------------------------");
        List casesValues = processQuery("SELECT cases FROM " + lambdaAndSchemaName + tableName4 + " where cases = 11");
        logger.info("casesValues : {}", casesValues);
        List dateColValues = processQuery("SELECT datecol FROM " + lambdaAndSchemaName + tableName4 + " where datecol = DATE '2016-02-05'");
        logger.info("dateColValues : {}", dateColValues);
        List dateTimeColExactMatch1  = processQuery("SELECT datetimecol FROM " + lambdaAndSchemaName + tableName4 + " where datetimecol = TIMESTAMP '2021-10-30 10:10:10'");
        logger.info("dateTimeColExactMatch1 : {}", dateTimeColExactMatch1);
        List dateTimeColExactMatch2  = processQuery("SELECT datetimecol FROM " + lambdaAndSchemaName + tableName4 + " where datetimecol = TIMESTAMP '2021-8-09 13:6:7.589'");
        logger.info("dateTimeColExactMatch1 : {}", dateTimeColExactMatch2);
        List dateTimeColLessThanCase  = processQuery("SELECT datetimecol FROM " + lambdaAndSchemaName + tableName4 + " where datetimecol <= TIMESTAMP '2021-08-11 15:06:07.458'");
        logger.info("dateTimeColLessThanCase : {}", dateTimeColLessThanCase);
        List dateTimeColGreaterThanCase  = processQuery("SELECT datetimecol FROM " + lambdaAndSchemaName + tableName4 + " where datetimecol >= TIMESTAMP '2021-08-11 15:06:07.458'");
        logger.info("dateTimeColGreaterThanCase : {}", dateTimeColGreaterThanCase);
        List timeStampColValues = processQuery("SELECT timstampcol FROM " + lambdaAndSchemaName + tableName5 + " where timstampcol ='2014-12-03T20:30:00.450Z'");
        logger.info("timeStampColValues : {}", timeStampColValues);
        List timeStampColValues2 = processQuery("SELECT timstampcol FROM " + lambdaAndSchemaName + tableName5 + " where timstampcol ='2014-12-03T12:30:00.450Z'");
        logger.info("timeStampColValues2 : {}", timeStampColValues2);
        List intColValues = processQuery("SELECT intcol FROM " + lambdaAndSchemaName + tableName + " where intcol = 50000000");
        logger.info("intColValues : {}", intColValues);
        List smallIntColValues = processQuery("SELECT smallintcol FROM " + lambdaAndSchemaName + tableName + " where smallintcol = 10000");
        logger.info("smallIntColValues : {}", smallIntColValues);
        List bigIntColValues = processQuery("SELECT bigintcol FROM " + lambdaAndSchemaName + tableName + " where bigintcol = 92233720368547567");
        logger.info("bigIntColValues : {}", bigIntColValues);
        List decimalColValues = processQuery("SELECT decimalcol FROM " + lambdaAndSchemaName + tableName + " where decimalcol = 3.33");
        logger.info("decimalColValues : {}", decimalColValues);
        List floatColValues = processQuery("SELECT floatcol FROM " + lambdaAndSchemaName + tableName + " where floatcol = 119.3464");
        logger.info("floatColValues : {}", floatColValues);
        List stringColValues = processQuery("SELECT stringcol FROM " + lambdaAndSchemaName + tableName + " where stringcol = 'Hello1'");
        logger.info("stringColValues : {}", stringColValues);
        List boolColValues = processQuery("SELECT boolcol FROM " + lambdaAndSchemaName + tableName + " where boolcol = true");
        logger.info("boolColValues : {}", boolColValues);
    }

    /**
     * Checking performance of the connector
     */
    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, "covid19_data");
        logger.info("throughput for Google Bigquery connector: "+ throughput);
    }


}
