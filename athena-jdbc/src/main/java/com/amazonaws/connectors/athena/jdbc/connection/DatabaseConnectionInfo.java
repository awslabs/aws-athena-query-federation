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

import org.apache.commons.lang3.Validate;

import java.util.Objects;

public class DatabaseConnectionInfo
{
    private final String driverClassName;
    private final int defaultPort;
    private final String templateConnectionString;

    public DatabaseConnectionInfo(final String driverClassName, final int defaultPort, final String templateConnectionString)
    {
        this.driverClassName = Validate.notBlank(driverClassName, "driverClassName must not be blank");
        this.defaultPort = defaultPort;
        this.templateConnectionString = Validate.notBlank(templateConnectionString, "templateConnectionString must not be blank");
    }

    public String getDriverClassName()
    {
        return driverClassName;
    }

    public int getDefaultPort()
    {
        return defaultPort;
    }

    public String getTemplateConnectionString()
    {
        return templateConnectionString;
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
                Objects.equals(getDriverClassName(), that.getDriverClassName()) &&
                Objects.equals(getTemplateConnectionString(), that.getTemplateConnectionString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getDriverClassName(), getDefaultPort(), getTemplateConnectionString());
    }
}
