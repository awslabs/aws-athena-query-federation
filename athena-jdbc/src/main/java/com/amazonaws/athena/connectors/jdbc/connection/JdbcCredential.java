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
 * Encapsulates database connection user name and password information.
 */
public class JdbcCredential
{
    private final String user;
    private final String password;

    /**
     * @param user Database user name.
     * @param password Database password.
     */
    public JdbcCredential(String user, String password)
    {
        this.user = Validate.notBlank(user, "User must not be blank");
        this.password = Validate.notBlank(password, "Password must not be blank");
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
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
        JdbcCredential that = (JdbcCredential) o;
        return Objects.equals(getUser(), that.getUser()) &&
                Objects.equals(getPassword(), that.getPassword());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getUser(), getPassword());
    }
}
