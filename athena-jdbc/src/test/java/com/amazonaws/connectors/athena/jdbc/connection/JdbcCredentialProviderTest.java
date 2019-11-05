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
package com.amazonaws.connectors.athena.jdbc.connection;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class JdbcCredentialProviderTest
{

    @Test
    public void getStaticCredential()
    {
        JdbcCredential expectedCredential = new JdbcCredential("testUser", "testPassword");
        JdbcCredentialProvider jdbcCredentialProvider = new StaticJdbcCredentialProvider(expectedCredential);

        Assert.assertEquals(expectedCredential, jdbcCredentialProvider.getCredential());
    }

    @Test
    public void getRdsSecretsCredential()
    {
        JdbcCredential expectedCredential = new JdbcCredential("testUser", "testPassword");
        JdbcCredentialProvider jdbcCredentialProvider = new RdsSecretsCredentialProvider("{\"username\": \"testUser\", \"password\": \"testPassword\"}");

        Assert.assertEquals(expectedCredential, jdbcCredentialProvider.getCredential());
    }

    @Test(expected = RuntimeException.class)
    public void getRdsSecretsCredentialIOException()
    {
        new RdsSecretsCredentialProvider("");
    }
}
