/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import com.amazonaws.athena.connectors.jdbc.credentials.RdsIamAuthConfiguration;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class DatabaseConnectionConfigTest
{
    private static final String CATALOG = "default";
    private static final String ENGINE = "postgres";
    private static final String HOSTNAME = "mydb.us-east-1.rds.amazonaws.com";
    private static final int PORT = 5432;
    private static final String USERNAME = "athena_iam";

    private static final RdsIamAuthConfiguration IAM_AUTH_CONFIGURATION = new RdsIamAuthConfiguration(
            HOSTNAME,
            PORT,
            USERNAME,
            Region.US_EAST_1);

    @Test
    public void isIamAuthEnabled_WhenIamConfigurationPresent_ReturnsTrue()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);

        assertTrue(databaseConnectionConfig.isIamAuthEnabled());
        assertEquals(IAM_AUTH_CONFIGURATION, databaseConnectionConfig.getIamAuthConfiguration());
    }

    @Test
    public void isIamAuthEnabled_WhenSecretConfigurationPresent_ReturnsFalse()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://hostname:5432/dev?${testSecret}",
                "testSecret");

        assertFalse(databaseConnectionConfig.isIamAuthEnabled());
    }

    @Test
    public void equals_WhenSameIamConfigurationValues_ReturnsTrue()
    {
        DatabaseConnectionConfig first = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);
        DatabaseConnectionConfig second = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void equals_WhenDifferentIamConfigurationValues_ReturnsFalse()
    {
        DatabaseConnectionConfig first = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);
        DatabaseConnectionConfig second = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://other.us-east-1.rds.amazonaws.com:5432/dev",
                new RdsIamAuthConfiguration(
                        "other.us-east-1.rds.amazonaws.com",
                        PORT,
                        USERNAME,
                        Region.US_EAST_1));

        assertNotEquals(first, second);
        assertNotEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void toString_WhenIamConfigurationPresent_IncludesIamConfiguration()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);

        assertTrue(databaseConnectionConfig.toString().contains("iamAuthConfiguration="));
    }
}
