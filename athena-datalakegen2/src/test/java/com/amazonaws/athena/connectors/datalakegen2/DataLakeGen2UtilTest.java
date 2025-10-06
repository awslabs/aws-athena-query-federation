/*-
 * #%L
 * athena-datalakegen2
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.datalakegen2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DataLakeGen2UtilTest
{
    @Test
    public void testCheckEnvironmentWithAzureServerlessUrl()
    {
        // Test with Azure serverless URL containing "ondemand" in hostname
        String serverlessUrl = "datalakegentwo://jdbc:sqlserver://myworkspace-ondemand.sql.azuresynapse.net:1433;database=mydatabase;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;";
        String result = DataLakeGen2Util.checkEnvironment(serverlessUrl);
        assertEquals("azureServerless", result);
    }

    @Test
    public void testCheckEnvironmentWithStandardUrl()
    {
        // Test with standard SQL Server URL (should not detect as serverless)
        String standardUrl = "datalakegentwo://jdbc:sqlserver://myserver.database.windows.net:1433;database=mydatabase;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
        String result = DataLakeGen2Util.checkEnvironment(standardUrl);
        assertNull(result);
    }

    @Test
    public void testCheckEnvironmentWithOnDemandInDatabaseName()
    {
        // Test with "ondemand" in database name but not hostname (should not detect as serverless)
        String urlWithOnDemandInDb = "datalakegentwo://jdbc:sqlserver://myserver.database.windows.net:1433;database=ondemand_database;";
        String result = DataLakeGen2Util.checkEnvironment(urlWithOnDemandInDb);
        assertNull(result);
    }

    @Test
    public void testCheckEnvironmentWithNullAndEmptyUrls()
    {
        // Test with null, empty, and invalid URLs
        assertNull(DataLakeGen2Util.checkEnvironment(null));
        assertNull(DataLakeGen2Util.checkEnvironment(""));
        assertNull(DataLakeGen2Util.checkEnvironment("   "));
        assertNull(DataLakeGen2Util.checkEnvironment("invalid-url-format"));
        assertNull(DataLakeGen2Util.checkEnvironment("https://ONDEMAND-server.database.windows.net:1433;database=mydatabase;"));
    }
}
