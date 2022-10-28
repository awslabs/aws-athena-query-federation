/*-
 * #%L
 * athena-cloudera-hive
 * %%
 * Copyright (C) 2019 - 2021 Amazon web services
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
package com.amazonaws.athena.connectors.cloudera.integ;


import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
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

public class HiveIntegTest extends IntegrationTestBase {

    private static final Logger logger = LoggerFactory.getLogger(HiveIntegTest.class);
    private static final String REGION ="region";
    private  String lambdaFunctionName;
    private static TestConfig testConfig;
    private static String tableName1;
    private static String tableName2;
    private static String tableName3;
    private static String tableName4;
    private static String lambdaAndSchemaName;
    private static String schemaName;
    private static  String region;

    static {
        testConfig = new TestConfig();
        region = testConfig.getStringItem(REGION).get();
        tableName1 = testConfig.getStringItem("hive_table_name1").get();
        tableName2 = testConfig.getStringItem("hive_table_name2").get();
        tableName3 = testConfig.getStringItem("hive_table_name3").get();
        tableName4 = testConfig.getStringItem("hive_table_name4").get();
        schemaName =testConfig.getStringItem("hive_schema_name").get();
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
    @BeforeClass
    @Override
    protected void setUp()
            throws Exception
    {
        super.setUp();
    }



    public HiveIntegTest()
    {
        logger.warn("Entered constructor");
        new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndSchemaName = "\"lambda:" + lambdaFunctionName + "\"." + schemaName + ".";
    }

    /**
     * Test to get all records for given a table.
     */
    @Test
    public void getTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List firstColValues = processQuery(String.format("select * from   \"lambda:%s\".%s.%s ;", lambdaFunctionName, schemaName, tableName1));
        Assert.assertEquals(firstColValues.size(), 999);
    }
    /**
     * Test to get total number of records for given a table.
     */
    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableCount = processQuery(String.format("select count(*) from   \"lambda:%s\".%s.%s;", lambdaFunctionName, schemaName ,tableName1));
        Assert.assertEquals(Integer.parseInt((String)tableCount.get(0)),144387);
    }
    /**
     * Test to get records for given a table if where condition match.
     */
    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = processQuery(String.format("select * from   \"lambda:%s\".%s.%s where %s = \'%s\';", lambdaFunctionName, schemaName ,tableName1 , "store_and_fwd_flag","Y"));
        Assert.assertEquals(firstColOfMatchedRows.size(),850);
    }

    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = processQuery(String.format("select * from   \"lambda:%s\".%s.%s where %s LIKE \'%s\';", lambdaFunctionName, schemaName ,tableName1 , "store_and_fwd_flag","%Y%"));
        Assert.assertEquals(firstColOfMatchedRows.size(),850);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List firstColValues = processQuery(String.format("select count(%s) from   \"lambda:%s\".%s.%s group by %s having %s = \'%s\';",
                "store_and_fwd_flag"  ,lambdaFunctionName ,schemaName , tableName1 , "store_and_fwd_flag" ,"store_and_fwd_flag","Y"));
        Assert.assertEquals(Integer.parseInt((String)firstColValues.get(0)),850);
    }

    @Test
    public void useUnion() {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List firstColValues = processQuery(String.format("select * from   \"lambda:%s\".%s.%s union all " +
                        "select * from   \"lambda:%s\".%s.%s where %s = \'%s\';",
                lambdaFunctionName ,schemaName , tableName1 , lambdaFunctionName ,schemaName , tableName1,"store_and_fwd_flag","Y" ));
        Assert.assertEquals(firstColValues.size(),999);
    }

    @Test
    public void useDistinct() {
        logger.info("-----------------------------------");
        logger.info("Executing useDistinct");
        logger.info("-----------------------------------");
        List firstColValues =processQuery(String.format("select distinct (%s) from  \"lambda:%s\".%s.%s where %s = \'%s\'  ;",
                "store_and_fwd_flag", lambdaFunctionName, schemaName, tableName1, "store_and_fwd_flag", "Y"));
        Assert.assertEquals(firstColValues.size(),1);

    }

    @Test
    public void useJoin() {
        logger.info("-----------------------------------");
        logger.info("Executing useJoin");
        logger.info("-----------------------------------");
        List firstColValues = processQuery(String.format("select * from   \"lambda:%s\".%s.%s t1, " +
                        " \"lambda:%s\".%s.%s t2 where t1.%s = t2.%s ;",
                lambdaFunctionName ,schemaName , tableName3 , lambdaFunctionName ,schemaName , tableName3,"year_time","year_time"));
        Assert.assertEquals(firstColValues.size(),4);
    }

    @Test
    public void useOrderBy() {
        logger.info("-----------------------------------");
        logger.info("Executing useOrderBy");
        logger.info("-----------------------------------");
        List varcharColAscValues = processQuery(String.format("select * from   \"lambda:%s\".%s.%s order by store_and_fwd_flag asc",lambdaFunctionName,schemaName,tableName1));
        Assert.assertEquals(varcharColAscValues.size(),999);
    }

    @Test
    public void selectPartitionedTable()
    {
        logger.info("-----------------------------------");
        logger.info("Executing partition selectTable");
        logger.info("-----------------------------------");

        List firstColValues = processQuery(String.format("select * from   \"lambda:%s\".%s.%s;", lambdaFunctionName, schemaName ,tableName2));
        Assert.assertEquals(firstColValues.size(),20);
    }

    @Test
    public void checkDataTypes() {
        logger.info("-----------------------------------");
        logger.info("Executing checkDataTypes");
        logger.info("-----------------------------------");
            List datetime2ColValues = processQuery("SELECT year_time FROM " + lambdaAndSchemaName + tableName3 + " where year_time >= TIMESTAMP '2023-07-07 00:00:00.000'");
            Assert.assertEquals(datetime2ColValues.size(), 2);
            List floatColValues = processQuery("SELECT growth_rate FROM " + lambdaAndSchemaName + tableName3 + " where growth_rate >= -45877458555845454.575");
            Assert.assertEquals(floatColValues.size(), 2);
            List bigintAsInt = processQuery("SELECT base_no FROM " + lambdaAndSchemaName + tableName3 + " where cast(base_no as int)=1 ");
            Assert.assertEquals(bigintAsInt.size(), 2);
            List bigintAsDouble = processQuery("SELECT base_no FROM " + lambdaAndSchemaName + tableName3 + " where cast(base_no as double)=1");
            Assert.assertEquals(bigintAsDouble.size(), 2);
            List bigintAsDecimal = processQuery("SELECT base_no FROM " + lambdaAndSchemaName + tableName3 + " where cast(base_no as decimal)=1 ");
            Assert.assertEquals(bigintAsDecimal.size(), 2);
            List useAggrSum = processQuery("select sum(regions_no) from " + lambdaAndSchemaName + tableName3);
            Assert.assertEquals(Integer.parseInt((String) useAggrSum.get(0)), 6);
    }
    @Test
    public void getThroughput() {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, tableName4);
        logger.info("Throughput for Hive Connector: "+ throughput);
    }
}
