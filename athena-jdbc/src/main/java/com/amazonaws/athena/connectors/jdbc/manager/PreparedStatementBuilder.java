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
package com.amazonaws.athena.connectors.jdbc.manager;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Encapsulates prepared statement string and parameters.
 */
public class PreparedStatementBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementBuilder.class);

    private String query;
    private List<String> parameters;
    private Connection connection;

    public PreparedStatementBuilder withParameters(final List<String> parameters)
    {
        this.parameters = Validate.notEmpty(parameters, "parameters must not be null");
        return this;
    }

    public PreparedStatementBuilder withQuery(final String query)
    {
        this.query = Validate.notBlank(query, "query must not be blank");
        return this;
    }

    public PreparedStatementBuilder withConnection(final Connection connection)
    {
        this.connection = Validate.notNull(connection, "connection must not be null");
        return this;
    }

    /**
     * Builds prepared statement from query string and string parameters.
     *
     * @return prepared statement. See {@link PreparedStatement}.
     */
    public PreparedStatement build()
    {
        Validate.notEmpty(parameters, "parameters must not be null");
        Validate.notBlank(query, "query must not be blank");
        Validate.notNull(connection, "connection must not be null");

        LOGGER.info("Running query {}", this.query);
        LOGGER.info("Parameters {}", this.parameters);

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(this.query);

            for (int i = 1; i <= parameters.size(); i++) {
                preparedStatement.setString(i, parameters.get(i - 1));
            }

            return preparedStatement;
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
        }
    }
}
