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
package com.amazonaws.athena.connectors.db2.integ;

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
 * Integration-tests for the Db2 (JDBC) connector using the Integration-test module.
 */
public class Db2IntegTest extends IntegrationTestBase {

    private static final Logger logger = LoggerFactory.getLogger(Db2IntegTest.class);
    private App theApp;
    private String lambdaFunctionName;

    private static TestConfig testConfig;
    private static String schemaName;
    private static String tableName1;
    private static  String tableName2;
    private static  String tableName3;
    private static  String tableName4;
    private static  String tableName5;
    private String lambdaAndSchemaName;

    static {
        testConfig = new TestConfig();
        System.setProperty("aws.region", testConfig.getStringItem("region").get());
        System.setProperty("aws.accessKeyId", testConfig.getStringItem("access_key").get());
        System.setProperty("aws.secretKey", testConfig.getStringItem("secret_key").get());
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName1 = testConfig.getStringItem("table_name").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        tableName3 = testConfig.getStringItem("table_name3").get();
        tableName4 = testConfig.getStringItem("table_name4").get();
        tableName5 = testConfig.getStringItem("table_name5").get();
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
    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public Db2IntegTest() throws Exception
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndSchemaName = "\"lambda:"+lambdaFunctionName+"\".DB2INST2.";
    }

    @Test
    public void getTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List<String> firstColValues = fetchDataSelect(schemaName,tableName1,lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List<String> tableCount = fetchDataSelectCountAll(schemaName,tableName1,lambdaFunctionName);
        logger.info("table count: {}", tableCount.get(0));
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List<String> firstColOfMatchedRows = fetchDataWhereClause(schemaName,tableName4,lambdaFunctionName, "varcol", "Ravi");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List<String> firstColOfMatchedRows = fetchDataWhereClauseLIKE(schemaName,tableName4,lambdaFunctionName, "CHARCOL", "A");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List<String> firstColValues = fetchDataGroupBy(schemaName,tableName4,lambdaFunctionName, "CHARCOL");
        logger.info("count(charcol) values: {}", firstColValues);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List<String> firstColValues = fetchDataGroupByHavingClause(schemaName,tableName4,lambdaFunctionName, "VARCOL", "Ankita");
        logger.info("count(varcharCol) Having 'KUMAR' values : {}", firstColValues);
    }

    @Test
    public void useJoin() {
        logger.info("-----------------------------------");
        logger.info("Executing useJoin");
        logger.info("-----------------------------------");
        List<String> firstColValues = fetchDataJoin(lambdaFunctionName, schemaName, tableName4, lambdaFunctionName, schemaName,
                tableName5, "VARCOL", "VARCOL");
        logger.info("firstColValues after join on varcharcol : {}", firstColValues);
    }

    @Test
    public void useOrderBy() {
        logger.info("-----------------------------------");
        logger.info("Executing useOrderBy");
        logger.info("-----------------------------------");
        List<String> varcharColAscValues = processQuery("select VARCOL from " + lambdaAndSchemaName + tableName4 + " order by VARCOL asc");
        logger.info("varcharColAscValues : {}", varcharColAscValues);
    }

    @Test
    public void checkDataTypes() {
        logger.info("-----------------------------------");
        logger.info("Executing checkDataTypes");
        logger.info("-----------------------------------");
        List<String> dateColValues = processQuery("SELECT datecol FROM " + lambdaAndSchemaName + tableName3 + " where datecol = DATE '2018-10-28'");
        logger.info("dateColValues : {}", dateColValues);
        List<String> timeColValues = processQuery("SELECT timecol FROM " + lambdaAndSchemaName + tableName3 + " where timecol = '13:30:05'");
        logger.info("timeColValues : {}", timeColValues);
        List<String> timestampColValues = processQuery("SELECT timestampcol FROM " + lambdaAndSchemaName + tableName3 + " where timestampcol >= TIMESTAMP  '2018-03-25 07:30:58.878'");
        logger.info("timestampColValues : {}", timestampColValues);
        List<String> doubleColValues = processQuery("SELECT doublecol FROM " + lambdaAndSchemaName + tableName2 + " where doublecol >= -123.46");
        logger.info("doubleColValues : {}", doubleColValues);
    }

    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, tableName1);
        logger.info("throughput for db2 connector: "+ throughput);
    }
}
