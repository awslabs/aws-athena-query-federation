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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * Static credential provider.
 */
public class StaticJdbcCredentialProvider
        implements JdbcCredentialProvider
{
    private final JdbcCredential jdbcCredential;

    /**
     * @param jdbcCredential JDBC credential. See {@link JdbcCredential}.
     */
    public StaticJdbcCredentialProvider(final JdbcCredential jdbcCredential)
    {
        this.jdbcCredential = Validate.notNull(jdbcCredential, "jdbcCredential must not be null.");

        if (StringUtils.isAnyBlank(jdbcCredential.getUser(), jdbcCredential.getPassword())) {
            throw new RuntimeException("User or password must not be blank.");
        }
    }

    @Override
    public JdbcCredential getCredential()
    {
        return this.jdbcCredential;
    }
}
