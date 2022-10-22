/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.integ;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration-tests for the SqlServer (JDBC) connector using the Integration-test module.
 */
public class SqlServerIntegTest extends IntegrationTestBase {

    private static final Logger logger = LoggerFactory.getLogger(SqlServerIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;

    private static TestConfig testConfig;
    private static String schemaName;
    private static String tableName1;
    private static  String tableName2;
    private static  String tableName3;
    private static  String tableName4;
    private static  String tableName5;
    private static  String tableName6;
    private static  String tableName7;
    private String lambdaAndSchemaName;

    static {
        testConfig = new TestConfig();
        System.setProperty("aws.region", testConfig.getStringItem("region").get());
        System.setProperty("aws.accessKeyId", testConfig.getStringItem("access_key").get());
        System.setProperty("aws.secretKey", testConfig.getStringItem("secret_key").get());
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName1 = testConfig.getStringItem("table_name1").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        tableName3 = testConfig.getStringItem("table_name3").get();
        tableName4 = testConfig.getStringItem("table_name4").get();
        tableName5 = testConfig.getStringItem("table_name5").get();
        tableName6 = testConfig.getStringItem("table_name6").get();
        tableName7 = testConfig.getStringItem("table_name7").get();
    }

    @Override
    protected void setUpTableData() {
        // No-op.
    }

    @Override
    protected void setUpStackData(Stack stack) {
        // No-op.
    }

    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars) {
        // No-op.
    }

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

    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        super.setUp();
    }

    public SqlServerIntegTest()
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
        List firstColValues = fetchDataSelect(schemaName,tableName1,lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableCount = fetchDataSelectCountAll(schemaName,tableName1,lambdaFunctionName);
        logger.info("table count: {}", tableCount.get(0));
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClause(schemaName,tableName1,lambdaFunctionName, "varcharCol", "KUMAR");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClauseLIKE(schemaName,tableName1,lambdaFunctionName, "charCol", "%Ravi%");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupBy(schemaName,tableName1,lambdaFunctionName, "charCol");
        logger.info("count(charcol) values: {}", firstColValues);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupByHavingClause(schemaName,tableName1,lambdaFunctionName, "varcharCol", "KUMAR");
        logger.info("count(varcharCol) Having 'KUMAR' values : {}", firstColValues);
    }

    @Test
    public void useUnion() {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataUnion(schemaName, tableName3, tableName4, lambdaFunctionName, lambdaFunctionName,
                "varcharcol", "Ravi");
        logger.info("union where varcharcol 'Ravi' values : {}", firstColValues);
    }

    @Test
    public void useDistinct() {
        logger.info("-----------------------------------");
        logger.info("Executing useDistinct");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataDistinct(lambdaFunctionName, schemaName, tableName1, "varcharcol", "varcharcol",
                "'KUMAR'");
        logger.info("distinct where varcharcol 'KUMAR' values : {}", firstColValues);
        assertEquals("Wrong number of DB records found.", 1, firstColValues.size());
        assertTrue("Data is not found.", firstColValues.contains("KUMAR"));
    }

    @Test
    public void useJoin() {
        logger.info("-----------------------------------");
        logger.info("Executing useJoin");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataJoin(lambdaFunctionName, schemaName, tableName3, lambdaFunctionName, schemaName,
                tableName4, "varcharcol", "varcharcol");
        logger.info("firstColValues after join on varcharcol : {}", firstColValues);
    }

    @Test
    public void useOrderBy() {
        logger.info("-----------------------------------");
        logger.info("Executing useOrderBy");
        logger.info("-----------------------------------");
        List varcharColAscValues = processQuery("select varcharcol from " + lambdaAndSchemaName + tableName1 + " order by varcharcol asc");
        logger.info("varcharColAscValues : {}", varcharColAscValues);
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
        List varcharAsInt = processQuery("SELECT passenger_count_vc FROM " + lambdaAndSchemaName + tableName1 + " where cast(passenger_count_vc as int)=4 ");
        logger.info("CastTest varcharAsInt : {}", varcharAsInt);
        List varcharAsDecimal = processQuery("SELECT trip_distance FROM " + lambdaAndSchemaName + tableName1 + " where cast(trip_distance as decimal)=3 ");
        logger.info("CastTest varcharAsDecimal : {}", varcharAsDecimal);
        List useAggrSum = processQuery("select sum(tinyintcol) from " + lambdaAndSchemaName + tableName7);
        logger.info("use aggrgate function SUM : {}", useAggrSum);
    }

    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, tableName2);
        logger.info("throughput for SqlServer connector: "+ throughput);
    }

}
