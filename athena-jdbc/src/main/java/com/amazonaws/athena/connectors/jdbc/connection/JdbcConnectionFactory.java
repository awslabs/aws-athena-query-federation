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

import java.sql.Connection;

/**
 * Factory abstracts creation of JDBC connection to database.
 */
public interface JdbcConnectionFactory
{
    /**
     * Retrieves database connection for a database type.
     *
     * @param jdbcCredentialProvider jdbc user and password provider.
     * @return JDBC connection. See {@link Connection}.
     */
    Connection getConnection(JdbcCredentialProvider jdbcCredentialProvider) throws Exception;
}
