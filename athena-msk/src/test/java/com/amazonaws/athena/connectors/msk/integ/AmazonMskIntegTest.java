/*-
 * #%L
 * athena-msk
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
package com.amazonaws.athena.connectors.msk.integ;

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

public class AmazonMskIntegTest extends IntegrationTestBase {
    private static final Logger logger = LoggerFactory.getLogger(AmazonMskIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;

    private static TestConfig testConfig;
    private static  String amazonmsk_db_name;
    private static  String tableName;
    private String lambdaAndamazonmsk_db_name;

    private static  String region;
    static {
        testConfig = new TestConfig();
        region = testConfig.getStringItem("region").get();
        amazonmsk_db_name = testConfig.getStringItem("amazonmsk_db_name").get();
        tableName = testConfig.getStringItem("table_name").get();
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

    public AmazonMskIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndamazonmsk_db_name = "\"lambda:"+lambdaFunctionName+"\""+amazonmsk_db_name+"\"";
    }

    @Test
    public void fetchTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchTableData");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelect(amazonmsk_db_name,tableName,lambdaFunctionName);
        logger.info("Total Records: {}", tableNames.size());
    }

    @Test
    public void fetchTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchTableCount");
        logger.info("-----------------------------------");
        List tableNames = fetchDataSelectCountAll(amazonmsk_db_name,tableName,lambdaFunctionName);
        logger.info("Total Records: {}", tableNames.size());
    }
    @Test
    public void fetchDataTypesCsv()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataTypesCsv");
        logger.info("-----------------------------------");
        //int test
        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where intcol=1023467";
        processQuery(queryTableCreation);
        //varchar test
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where varcharcol='JitheNdar'";
        processQuery(queryTableCreation);
        //boolean test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where booleancol=true";
        processQuery(queryTableCreation);
        //bigint test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where bigintcol= 78945612";
        processQuery(queryTableCreation);
        //double test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where doublecol=912.2290";
        processQuery(queryTableCreation);
        //smallint test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where smallintcol=12";
        processQuery(queryTableCreation);
        //tinyint test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypecsv where tinyintcol=123";
        processQuery(queryTableCreation);
    }
    @Test
    public void fetchDataTypesJson(){
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataTypesJson");
        logger.info("-----------------------------------");
        //int test
        String  queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where intcol=2";
        processQuery(queryTableCreation);
        //varchar test
        queryTableCreation = "select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where varcharcol='Namrata'";
        processQuery(queryTableCreation);
        //boolean test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where booleancol=true";
        processQuery(queryTableCreation);
        //bigint test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where bigintcol=78945654";
        processQuery(queryTableCreation);
        //double test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where doublecol=1234546.1234556";
        processQuery(queryTableCreation);
        //smallint test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where smallintcol=12245";
        processQuery(queryTableCreation);
        //tinyint test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where tinyintcol=123";
        processQuery(queryTableCreation);
        //date test
        queryTableCreation="select * from \"lambda:" + lambdaFunctionName + "\"."+ "default" + "." + "datatypejson where datecol=date('2022-09-13')";
        processQuery(queryTableCreation);
    }
    @Test
    public void fetchDataWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClause");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClause(amazonmsk_db_name,tableName,lambdaFunctionName,"name","varun");

        logger.info("Tables: {}", tableNames);
    }
    @Test
    public void fetchDataWhereClauseLIKE()
    {
        logger.info("-----------------------------------");
        logger.info("Executing fetchDataWhereClauseLIKE");
        logger.info("-----------------------------------");
        List tableNames = fetchDataWhereClauseLIKE(amazonmsk_db_name,tableName,lambdaFunctionName,"name","namrata");

        logger.info("Tables: {}", tableNames);
    }
}
