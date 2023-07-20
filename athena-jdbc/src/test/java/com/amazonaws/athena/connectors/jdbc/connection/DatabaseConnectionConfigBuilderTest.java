/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DatabaseConnectionConfigBuilderTest
{
    private static final String CONNECTION_STRING1 = "mysql://jdbc:mysql://hostname/${testSecret}";
    private static final String CONNECTION_STRING2 = "postgres://jdbc:postgresql://hostname/user=testUser&password=testPassword";
    private static final String CONNECTION_STRING3 = "redshift://jdbc:redshift://hostname:5439/dev?${arn:aws:secretsmanager:us-east-1:1234567890:secret:redshift/user/secret}";
    private static final String CONNECTION_STRING4 = "postgres://jdbc:postgresql://hostname:5439/dev?${arn:aws:secretsmanager:us-east-1:1234567890:secret:postgresql/user/secret}";

    @Test
    public void build()
    {
        DatabaseConnectionConfig defaultConnection = new DatabaseConnectionConfig("default", "postgres",
                "jdbc:postgresql://hostname/user=testUser&password=testPassword");
        DatabaseConnectionConfig expectedDatabase1 = new DatabaseConnectionConfig("testCatalog1", "postgres",
                "jdbc:postgresql://hostname:5439/dev?${arn:aws:secretsmanager:us-east-1:1234567890:secret:postgresql/user/secret}",
                "arn:aws:secretsmanager:us-east-1:1234567890:secret:postgresql/user/secret");
        DatabaseConnectionConfig expectedDatabase2 = new DatabaseConnectionConfig("testCatalog2", "postgres",
                "jdbc:postgresql://hostname/user=testUser&password=testPassword");

        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder()
                .engine("postgres")
                .properties(ImmutableMap.of(
                        "default", CONNECTION_STRING2,
                        "testCatalog1_connection_string", CONNECTION_STRING4,
                        "testCatalog2_connection_string", CONNECTION_STRING2))
                .build();

        Assert.assertEquals(Arrays.asList(defaultConnection, expectedDatabase1, expectedDatabase2), databaseConnectionConfigs);
    }

    @Test(expected = RuntimeException.class)
    public void buildMultipleDatabasesFails()
    {
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder()
                .properties(ImmutableMap.of(
                        "default", CONNECTION_STRING2,
                        "testCatalog1_connection_string", CONNECTION_STRING1,
                        "testCatalog2_connection_string", CONNECTION_STRING2,
                        "testCatalog3_connection_string", CONNECTION_STRING3))
                .build();
    }

    @Test(expected = RuntimeException.class)
    public void buildInvalidConnectionString()
    {
        new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("default", "malformedUrl")).build();
    }

    @Test(expected = RuntimeException.class)
    public void buildWithNoDefault()
    {
        new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("testDb_connection_string", CONNECTION_STRING1)).build();
    }

    @Test(expected = RuntimeException.class)
    public void buildMalformedConnectionString()
    {
        new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("testDb_connection_string", null)).build();
    }

    @Test
    public void invalidSecretsSyntaxTest()
    {
        String engine = "redshift";
        List<String> invalidConnectionStrings = Arrays.asList(
                "redshift://jdbc:redshift://hostname:5439/dev?${inv&li$dSecret}",
                "redshift://jdbc:redshift://hostname:5439/dev?${an*therOne))}",
                "redshift://jdbc:redshift://hostname:5439/dev?${in^a]i?}",
                "redshift://jdbc:redshift://hostname:5439/dev?${an*therOne))}");
        for (String connection: invalidConnectionStrings) {
                Assert.assertThrows(RuntimeException.class, () -> new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("testDb_connection_string", connection)).engine(engine).build());
        }
    }

    @Test
    public void validSecretsSyntaxTest()
    {
        String engine = "redshift";
        String connectionString1 = "redshift://jdbc:redshift://hostname:5439/dev?${spec.@/Ch@rac+=r_}";
        String connectionString2 = "redshift://jdbc:redshift://hostname:5439/dev?${rds!service-linked-secret}";
        String connectionString3 = "redshift://jdbc:redshift://hostname:5439/dev?${redshift:credentials-secret}";
        String connectionString4 = "redshift://jdbc:redshift://hostname:5439/dev?${opsworks-cm:credentials12}";
        String[] secrets = new String[]{"spec.@/Ch@rac+=r_", "rds!service-linked-secret", "redshift:credentials-secret", "opsworks-cm:credentials12"};
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder()
                .engine(engine)
                .properties(ImmutableMap.of(
                        "default", connectionString1,
                        "testCatalog2_connection_string", connectionString2,
                        "testCatalog3_connection_string", connectionString3,
                        "testCatalog4_connection_string", connectionString4))
                .build();
        Assert.assertEquals(secrets.length, databaseConnectionConfigs.size());
        for (int i = 0; i < databaseConnectionConfigs.size(); i++) {
                Assert.assertEquals(secrets[i], databaseConnectionConfigs.get(i).getSecret());
        }
    }
}
