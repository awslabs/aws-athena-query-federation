/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

public class PostGreSqlConstants
{
    public static final String POSTGRES_NAME = "postgres";
    public static final String POSTGRESQL_DRIVER_CLASS = "org.postgresql.Driver";
    public static final int POSTGRESQL_DEFAULT_PORT = 5432;
    public static final String POSTGRES_QUOTE_CHARACTER = "\"";

    private PostGreSqlConstants() {}
}
