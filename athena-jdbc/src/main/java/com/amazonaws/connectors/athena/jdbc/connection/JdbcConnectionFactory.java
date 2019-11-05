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

import java.sql.Connection;

/**
 * Factory abstracts creation of JDBC connection to database.
 */
public interface JdbcConnectionFactory
{
    /**
     * Used to connect with standard databases. Default port for the engine will be used if value is negative.
     *
     * @param jdbcCredentialProvider jdbc user and password provider
     * @return {@link Connection}
     */
    Connection getConnection(JdbcCredentialProvider jdbcCredentialProvider);

    /**
     * Databases supported to create JDBC connection. These would be connector names as well.
     */
    enum DatabaseEngine
    {
        MYSQL("mysql"),
        POSTGRES("postgres"),
        ORACLE("oracle"),
        SQLSERVER("sqlserver");

        private final String dbName;

        DatabaseEngine(final String dbName)
        {
            this.dbName = dbName;
        }

        public String getDbName()
        {
            return this.dbName;
        }
    }
}
