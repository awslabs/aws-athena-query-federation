/*-
 * #%L
 * athena-synapse
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcCasingSqlDialect;
import org.apache.calcite.sql.SqlDialect;

/**
 * Azure Synapse-specific SQL dialect with catalog casing filter support. Synapse uses the Microsoft SQL Server
 * (T-SQL) engine and JDBC driver, so it renders Substrait-derived query plans with the Calcite MSSQL dialect and
 * uses square brackets ({@code []}) for identifier quoting, matching the shipped SQL Server connector.
 */
public class SynapseDialect extends JdbcCasingSqlDialect
{
    public static final SqlDialect DEFAULT = org.apache.calcite.sql.dialect.MssqlSqlDialect.DEFAULT;

    public SynapseDialect(boolean catalogCasingFilter)
    {
        super(DatabaseProduct.MSSQL, "[", "]", catalogCasingFilter);
    }
}
