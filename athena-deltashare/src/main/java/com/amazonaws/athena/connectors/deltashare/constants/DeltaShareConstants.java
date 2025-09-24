/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.constants;

/**
 * Constants for Delta Share connector configuration and protocol definitions.
 */
public final class DeltaShareConstants
{
    public static final String SOURCE_TYPE = "deltashare";
    
    public static final String ENDPOINT_PROPERTY = "endpoint";
    public static final String TOKEN_PROPERTY = "token";
    public static final String SHARE_NAME_PROPERTY = "share_name";
    
    public static final String SHARES_ENDPOINT = "shares";
    public static final String SCHEMAS_ENDPOINT = "shares/%s/schemas";
    public static final String TABLES_ENDPOINT = "shares/%s/schemas/%s/tables";
    public static final String TABLE_METADATA_ENDPOINT = "shares/%s/schemas/%s/tables/%s/metadata";
    public static final String QUERY_TABLE_ENDPOINT = "shares/%s/schemas/%s/tables/%s/query";
    
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String BEARER_PREFIX = "Bearer ";
    public static final String APPLICATION_JSON = "application/json";
    
    public static final String SHARE_PROPERTY = "share";
    public static final String SCHEMA_PROPERTY = "schema";
    public static final String TABLE_PROPERTY = "table";
    public static final String PARTITION_INFO_PROPERTY = "partition_info";
    
    public static final long LAMBDA_TEMP_LIMIT = 480L * 1024 * 1024;
    public static final long FILE_SIZE_THRESHOLD = 536870912L;
    public static final long ROW_GROUP_SIZE_THRESHOLD = 100L * 1024 * 1024;
    
    private DeltaShareConstants() {}
}
