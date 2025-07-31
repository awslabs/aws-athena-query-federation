/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class FASToken
{
    private final String accessToken;

    private final String secretToken;

    private final String securityToken;

    @JsonCreator
    public FASToken(@JsonProperty("accessToken") String accessToken,
                    @JsonProperty("secretToken") String secretToken,
                    @JsonProperty("securityToken") String securityToken)
    {
        this.accessToken = accessToken;
        this.secretToken = secretToken;
        this.securityToken = securityToken;
    }

    @JsonProperty
    public String getAccessToken()
    {
        return accessToken;
    }

    @JsonProperty
    public String getSecretToken()
    {
        return secretToken;
    }

    @JsonProperty
    public String getSecurityToken()
    {
        return securityToken;
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

        FASToken that = (FASToken) o;

        return accessToken.equals(that.accessToken) &&
                secretToken.equals(that.secretToken) &&
                securityToken.equals(that.securityToken);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessToken, secretToken, securityToken);
    }
}
