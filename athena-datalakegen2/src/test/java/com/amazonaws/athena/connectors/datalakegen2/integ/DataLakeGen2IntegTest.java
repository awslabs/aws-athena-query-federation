/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2.integ;

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
 * Integration-tests for the ADLS Gen2 (JDBC) connector using the Integration-test module.
 */
public class DataLakeGen2IntegTest extends IntegrationTestBase {

    private static final Logger logger = LoggerFactory.getLogger(DataLakeGen2IntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;

    private static TestConfig testConfig;
    private static  String schemaName;
    private static  String tableName;
    private static String tableName1;
    private static String tableName2;
    private String lambdaAndSchemaName;

    static {
        testConfig = new TestConfig();
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName = testConfig.getStringItem("table_name").get();
        tableName1 = testConfig.getStringItem("table_name1").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        System.setProperty("aws.region", testConfig.getStringItem("region").get());
        System.setProperty("aws.accessKeyId", testConfig.getStringItem("access_key").get());
        System.setProperty("aws.secretKey", testConfig.getStringItem("secret_key").get());
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

    public DataLakeGen2IntegTest()
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
        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(schemaName,tableName,lambdaFunctionName);
        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(schemaName,tableName,lambdaFunctionName,"iso2","RU");

        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(schemaName,tableName,lambdaFunctionName,"iso2","RU");

        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupBy(schemaName,tableName,lambdaFunctionName,"iso2");
        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List tableNames = fetchDataGroupByHavingClause(schemaName,tableName,lambdaFunctionName,"iso2","RU");
        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void useUnion()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List tableNames = fetchDataUnion(schemaName,tableName,tableName1,lambdaFunctionName,lambdaFunctionName,
                "iso2","RU");
        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, tableName);
        logger.info("throughput for ADLS Gen2 connector: "+ throughput);
    }

    @Test
    public void checkDataTypes() {
        logger.info("-----------------------------------");
        logger.info("Executing checkDataTypes");
        logger.info("-----------------------------------");
        List floatValues = processQuery("SELECT * FROM " + lambdaAndSchemaName + tableName2 + " where trip_distance = 0.86 limit 10");
        logger.info("floatValues : {}", floatValues);
        List intValues = processQuery("SELECT * FROM " + lambdaAndSchemaName + tableName2 + " where passenger_count = 5 limit 10");
        logger.info("intValues : {}", intValues);
        List smallIntValues = processQuery("SELECT * FROM " + lambdaAndSchemaName + tableName2 + " where payment_type = 1 limit 10");
        logger.info("smallIntValues : {}", smallIntValues);
        List varcharValues = processQuery("SELECT * FROM " + lambdaAndSchemaName + tableName2 + " where vendorid = '2' limit 10");
        logger.info("varcharValues : {}", varcharValues);
    }

}

