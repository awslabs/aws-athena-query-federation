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
 * Encapsulates database and JDBC driver defaults for a database type.
 */
public class DatabaseConnectionInfo
{
    private final String driverClassName;
    private final int defaultPort;

    /**
     * @param driverClassName JDBC driver class name.
     * @param defaultPort Database default port.
     */
    public DatabaseConnectionInfo(final String driverClassName, final int defaultPort)
    {
        this.driverClassName = Validate.notBlank(driverClassName, "driverClassName must not be blank");
        this.defaultPort = defaultPort;
    }

    public String getDriverClassName()
    {
        return driverClassName;
    }

    public int getDefaultPort()
    {
        return defaultPort;
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
        DatabaseConnectionInfo that = (DatabaseConnectionInfo) o;
        return getDefaultPort() == that.getDefaultPort() &&
                Objects.equals(getDriverClassName(), that.getDriverClassName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getDriverClassName(), getDefaultPort());
    }
}
