/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;

import static com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory.SECRET_NAME_PATTERN;

public class GenericJdbcConnectionFactoryTest
{
    @Test
    public void matchSecretNamePattern()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertTrue(secretMatcher.find());
    }

    @Test
    public void matchIncorrectSecretNamePattern()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=*_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertFalse(secretMatcher.find());
    }
}
