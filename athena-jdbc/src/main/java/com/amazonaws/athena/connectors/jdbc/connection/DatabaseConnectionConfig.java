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

import org.apache.commons.lang3.Validate;

import java.util.Objects;

/**
 * Encapsulates database JDBC connection configuration.
 */
public class DatabaseConnectionConfig
{
    private String catalog;
    private final JdbcConnectionFactory.DatabaseEngine type;
    private final String jdbcConnectionString;
    private String secret;

    /**
     * Creates configuration for credentials managed by AWS Secrets Manager.
     *
     * @param catalog catalog name passed by Athena.
     * @param type database type. See {@link JdbcConnectionFactory.DatabaseEngine}.
     * @param jdbcConnectionString jdbc native database connection string of database type.
     * @param secret AWS Secrets Manager secret name.
     */
    public DatabaseConnectionConfig(final String catalog, final JdbcConnectionFactory.DatabaseEngine type, final String jdbcConnectionString, final String secret)
    {
        this.catalog = Validate.notBlank(catalog, "catalog must not be blank");
        this.type = Validate.notNull(type, "type must not be blank");
        this.jdbcConnectionString = Validate.notBlank(jdbcConnectionString, "jdbcConnectionString must not be blank");
        this.secret = Validate.notBlank(secret, "secret must not be blank");
    }

    /**
     * Creates configuration for credentials passed through JDBC connection string.
     *
     * @param catalog catalog name passed by Athena.
     * @param type database type. See {@link JdbcConnectionFactory.DatabaseEngine}.
     * @param jdbcConnectionString jdbc native database connection string of database type.
     */
    public DatabaseConnectionConfig(final String catalog, final JdbcConnectionFactory.DatabaseEngine type, final String jdbcConnectionString)
    {
        this.catalog = Validate.notBlank(catalog, "catalog must not be blank");
        this.type = Validate.notNull(type, "type must not be blank");
        this.jdbcConnectionString = Validate.notBlank(jdbcConnectionString, "jdbcConnectionString must not be blank");
    }

    public JdbcConnectionFactory.DatabaseEngine getType()
    {
        return type;
    }

    public String getJdbcConnectionString()
    {
        return jdbcConnectionString;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSecret()
    {
        return secret;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseConnectionConfig that = (DatabaseConnectionConfig) o;
        return Objects.equals(getCatalog(), that.getCatalog()) &&
                getType() == that.getType() &&
                Objects.equals(getJdbcConnectionString(), that.getJdbcConnectionString()) &&
                Objects.equals(getSecret(), that.getSecret());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getCatalog(), getType(), getJdbcConnectionString(), getSecret());
    }

    @Override
    public String toString()
    {
        return "DatabaseConnectionConfig{" +
                "catalog='" + catalog + '\'' +
                ", type=" + type +
                ", jdbcConnectionString='" + jdbcConnectionString + '\'' +
                ", secret='" + secret + '\'' +
                '}';
    }
}
