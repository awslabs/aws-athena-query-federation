/*-
 * #%L
 * trianz-bigquery-athena-sdk
 * %%
 * Copyright (C) 2019 - 2022 Trianz
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
package com.amazonaws.athena.connectors.gcs.integ;

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

public class GcsIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(GcsIntegTest.class);
    private final String lambdaFunctionName;
    private static final String TABLE_NAME = "table_name";

    private static final TestConfig testConfig;
    private static final String schemaName;
    private static final String tableName2;
    private static final String tableName4;
    private static final String tableName5;
    private static final String tableName;
    private static String lambdaAndSchemaName;

    static {
        testConfig = new TestConfig();
        tableName = testConfig.getStringItem(TABLE_NAME).get();
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        tableName4 = testConfig.getStringItem("table_name4").get();
        tableName5 = testConfig.getStringItem("table_name5").get();
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     *
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
    protected void setUpTableData()
    {
        // No-op.
    }

    /**
     * Sets up connector-specific Cloud Formation resource.
     *
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

    public GcsIntegTest()
    {
        super();
        logger.warn("Entered constructor");
        App theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndSchemaName = "\"lambda:" + lambdaFunctionName + "\"." + schemaName + ".";
    }

    @Test
    public void getFirstTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataSelect(schemaName, tableName, lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void getTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataSelect(schemaName, tableName, lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableCount = fetchDataSelectCountAll(schemaName, tableName, lambdaFunctionName);
        logger.info("table count: {}", tableCount.get(0));
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClause(schemaName, tableName, lambdaFunctionName, "name", "Azam");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClauseLIKE(schemaName, tableName, lambdaFunctionName, "name", "%rav");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupBy(schemaName, tableName4, lambdaFunctionName, "country");
        logger.info("count(country) values: {}", firstColValues);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupByHavingClause(schemaName, tableName4, lambdaFunctionName, "country", "Japan");
        logger.info("count(country) Having 'Japan' values : {}", firstColValues);
    }

    @Test
    public void useUnion()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataUnion(schemaName, tableName4, tableName5, lambdaFunctionName, lambdaFunctionName, "country", "Japan");
        logger.info("union where country 'Japan' values : {}", firstColValues);
    }

    @Test
    public void checkDataTypes()
    {
        logger.info("-----------------------------------");
        logger.info("Executing checkDataTypes");
        logger.info("-----------------------------------");
        List bigIntColValues = processQuery("SELECT id FROM " + lambdaAndSchemaName + tableName + " where id = 11");
        logger.info("bigIntColValues : {}", bigIntColValues);

        List dateColValues = processQuery("SELECT dob FROM " + lambdaAndSchemaName + tableName + " where dob = DATE '1990-01-01'");
        logger.info("dateColValues : {}", dateColValues);

        List dateColValuesGreaterThan = processQuery("SELECT dob FROM " + lambdaAndSchemaName + tableName + " where dob > DATE '1985-01-01'");
        logger.info("dateColValuesGreaterThan : {}", dateColValuesGreaterThan);

        List dateColValuesLessThan = processQuery("SELECT dob FROM " + lambdaAndSchemaName + tableName + " where dob < DATE '1990-01-01'");
        logger.info("dateColValuesLessThan : {}", dateColValuesLessThan);

        List intColValues = processQuery("SELECT id FROM " + lambdaAndSchemaName + tableName + " where id = 10");
        logger.info("intColValues : {}", intColValues);

        List doubleColValues = processQuery("SELECT salary FROM " + lambdaAndSchemaName + tableName + " where salary = 3200.5");
        logger.info("doubleColValues : {}", doubleColValues);

        List stringColValues = processQuery("SELECT name FROM " + lambdaAndSchemaName + tableName + " where name = 'Azam'");
        logger.info("stringColValues : {}", stringColValues);

        List stringNotEqualColValues = processQuery("SELECT name FROM " + lambdaAndSchemaName + tableName + " where name <> 'Azam'");
        logger.info("stringNotEqualColValues : {}", stringNotEqualColValues);

        List stringColValuesLike = processQuery("SELECT name FROM " + lambdaAndSchemaName + tableName + " where name like '%rav'");
        logger.info("stringColValuesLike : {}", stringColValuesLike);

        List booleanColValueEqual = processQuery("SELECT onboarded FROM " + lambdaAndSchemaName + tableName + " where onboarded = true");
        logger.info("booleanColValueEqual : {}", booleanColValueEqual);

        List intColValueEqual = processQuery("SELECT sequence FROM " + lambdaAndSchemaName + tableName + " where sequence = 2");
        logger.info("intColValueEqual : {}", intColValueEqual);

        List floatColValueEqual = processQuery("SELECT sequence FROM " + lambdaAndSchemaName + tableName + " where sequence = 2");
        logger.info("floatColValueEqual : {}", floatColValueEqual);
    }

    /**
     * Checking performance of the connector
     */
    @Test
    public void getThroughput()
    {
        float throughput = calculateThroughput(lambdaFunctionName, schemaName, tableName2);
        logger.info("throughput for Athena GCS connector: " + throughput);
    }
}
