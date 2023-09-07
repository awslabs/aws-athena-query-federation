/*-
 * #%L
 * athena-hortonworks-hive
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.hortonworks;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

public class HiveFederationExpressionParser extends JdbcFederationExpressionParser
{
    public HiveFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    @Override
    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        return Joiner.on(", ").join(arguments);
    }
}
