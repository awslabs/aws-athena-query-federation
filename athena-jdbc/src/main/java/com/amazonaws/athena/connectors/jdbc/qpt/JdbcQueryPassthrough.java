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
package com.amazonaws.athena.connectors.jdbc.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.qpt.QueryPassthroughSignature;

import java.util.ArrayList;
import java.util.List;

/**
 * A Singleton class that implements QPT signature interface to define
 * the JDBC Query Passthrough Function's signature that will be used
 * to inform the engine how to define QPT Function for a JDBC connector
 */
public class JdbcQueryPassthrough implements QueryPassthroughSignature
{
    private String name;
    private String domain;
    private List<String> arguments;
    private static JdbcQueryPassthrough instance;

    static
    {
        JdbcQueryPassthrough.getInstance();
    }

    private JdbcQueryPassthrough()
    {
        this.name = "query";
        this.domain = "system";
        this.arguments = new ArrayList<>(1);
        arguments.add("QUERY");
    }

    public static JdbcQueryPassthrough getInstance()
    {
        if (instance == null) {
            instance = new JdbcQueryPassthrough();
        }
        return instance;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getDomain()
    {
        return domain;
    }

    @Override
    public List<String> arguments()
    {
        return arguments;
    }

    public String getQueryArgument()
    {
        return this.arguments.get(0);
    }
}
