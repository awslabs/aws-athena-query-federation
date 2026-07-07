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

import com.amazonaws.athena.connector.credentials.RdsIamAuthConfiguration;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

public class RdsIamConnectionStringParserTest
{
    @Test
    public void parse_WhenIamAuthDisabled_ReturnsOriginalConnectionString()
    {
        final String jdbcConnectionString = "jdbc:postgresql://hostname:5432/dev?user=testUser&password=testPassword";
        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertFalse(parsed.isIamAuthEnabled());
        Assert.assertEquals(jdbcConnectionString, parsed.getJdbcConnectionString());
        Assert.assertNull(parsed.getIamAuthConfiguration());
    }

    @Test
    public void parse_WhenIamAuthEnabled_SanitizesConnectionStringAndBuildsConfiguration()
    {
        final String jdbcConnectionString =
                "jdbc:postgresql://mydb.cluster-abc.us-east-1.rds.amazonaws.com:5432/dev?iamAuth=true&user=athena_iam";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertTrue(parsed.isIamAuthEnabled());
        Assert.assertEquals(
                "jdbc:postgresql://mydb.cluster-abc.us-east-1.rds.amazonaws.com:5432/dev",
                parsed.getJdbcConnectionString());

        RdsIamAuthConfiguration iamAuthConfiguration = parsed.getIamAuthConfiguration();
        Assert.assertEquals("mydb.cluster-abc.us-east-1.rds.amazonaws.com", iamAuthConfiguration.getHostname());
        Assert.assertEquals(5432, iamAuthConfiguration.getPort());
        Assert.assertEquals("athena_iam", iamAuthConfiguration.getUsername());
        Assert.assertEquals(Region.US_EAST_1, iamAuthConfiguration.getRegion());
    }

    @Test
    public void parse_WhenRegionOverrideProvided_UsesExplicitRegion()
    {
        final String jdbcConnectionString =
                "jdbc:postgresql://custom-host.example.com:5432/dev?iamAuth=true&user=athena_iam&region=us-west-2";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertEquals(Region.US_WEST_2, parsed.getIamAuthConfiguration().getRegion());
    }

    @Test
    public void parse_WhenJdbcUrlDoesNotMatch_ReturnsOriginalConnectionString()
    {
        final String jdbcConnectionString = "not-a-jdbc-url";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertFalse(parsed.isIamAuthEnabled());
        Assert.assertEquals(jdbcConnectionString, parsed.getJdbcConnectionString());
    }

    @Test
    public void parse_WhenNoQueryString_ReturnsOriginalConnectionString()
    {
        final String jdbcConnectionString = "jdbc:postgresql://hostname:5432/dev";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertFalse(parsed.isIamAuthEnabled());
        Assert.assertEquals(jdbcConnectionString, parsed.getJdbcConnectionString());
    }

    @Test
    public void parse_WhenIamAuthFalse_ReturnsOriginalConnectionString()
    {
        final String jdbcConnectionString = "jdbc:postgresql://hostname:5432/dev?iamAuth=false&user=athena_iam";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertFalse(parsed.isIamAuthEnabled());
        Assert.assertEquals(jdbcConnectionString, parsed.getJdbcConnectionString());
    }

    @Test(expected = AthenaConnectorException.class)
    public void parse_WhenIamAuthEnabledWithoutUser_ThrowsAthenaConnectorException()
    {
        RdsIamConnectionStringParser.parse("jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev?iamAuth=true");
    }

    @Test
    public void parse_WhenUsernameParamUsed_ReturnsUsernameFromUsernameKey()
    {
        final String jdbcConnectionString =
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev?iamAuth=true&username=athena_iam";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertEquals("athena_iam", parsed.getIamAuthConfiguration().getUsername());
    }

    @Test(expected = AthenaConnectorException.class)
    public void parse_WhenRegionCannotBeResolved_ThrowsAthenaConnectorException()
    {
        RdsIamConnectionStringParser.parse("jdbc:postgresql://custom-host.example.com:5432/dev?iamAuth=true&user=athena_iam");
    }

    @Test(expected = AthenaConnectorException.class)
    public void parse_WhenPortMissing_ThrowsAthenaConnectorException()
    {
        RdsIamConnectionStringParser.parse(
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com/dev?iamAuth=true&user=athena_iam");
    }

    @Test
    public void parse_WhenOnlyIamControlParamsRemain_ReturnsBaseUrlWithoutQuery()
    {
        final String jdbcConnectionString =
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev?iamAuth=true&user=athena_iam&password=secret";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertEquals("jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev", parsed.getJdbcConnectionString());
    }

    @Test
    public void parse_WhenTrailingQuestionMarkOnly_ReturnsOriginalConnectionString()
    {
        final String jdbcConnectionString = "jdbc:postgresql://hostname:5432/dev?";

        RdsIamConnectionStringParser.ParsedJdbcConnection parsed =
                RdsIamConnectionStringParser.parse(jdbcConnectionString);

        Assert.assertFalse(parsed.isIamAuthEnabled());
        Assert.assertEquals(jdbcConnectionString, parsed.getJdbcConnectionString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_WhenConnectionStringBlank_ThrowsException()
    {
        RdsIamConnectionStringParser.parse(" ");
    }
}
