/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake.utils;

/**
 * Enum representing different Snowflake authentication types.
 */
public enum SnowflakeAuthType
{
    /**
     * Password-based authentication (default)
     */
    SNOWFLAKE("snowflake"),

    /**
     * Key-pair authentication using JWT
     */
    SNOWFLAKE_JWT("snowflake_jwt"),

    /**
     * OAuth authentication
     */
    OAUTH("oauth");

    private final String value;

    SnowflakeAuthType(String value)
    {
        this.value = value;
    }

    /**
     * Gets the string value of the authentication type.
     * 
     * @return The string value
     */
    public String getValue()
    {
        return value;
    }

    /**
     * Converts a string to the corresponding SnowflakeAuthType enum.
     * 
     * @param authType The string representation of the auth type
     * @return The corresponding SnowflakeAuthType enum
     * @throws IllegalArgumentException if the auth type is not supported
     */
    public static SnowflakeAuthType fromString(String authType)
    {
        if (authType == null) {
            throw new IllegalArgumentException("Auth type cannot be null");
        }

        for (SnowflakeAuthType type : values()) {
            if (type.value.equals(authType)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unsupported authentication type: " + authType);
    }

    @Override
    public String toString()
    {
        return value;
    }
}
