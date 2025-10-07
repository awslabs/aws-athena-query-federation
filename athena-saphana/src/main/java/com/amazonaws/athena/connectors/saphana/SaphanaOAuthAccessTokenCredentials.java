/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import org.apache.commons.lang3.Validate;

import java.util.Map;

/**
 * Saphana-specific OAuth credentials implementation.
 * Unlike the standard OAuthAccessTokenCredentials which uses "accessToken" as the property key,
 * Saphana requires the OAuth access token to be provided as the "password" property.
 */
public class SaphanaOAuthAccessTokenCredentials implements Credentials
{
    private final String accessToken;

    public SaphanaOAuthAccessTokenCredentials(String accessToken)
    {
        this.accessToken = Validate.notBlank(accessToken, "Access token must not be blank");
    }

    public String getAccessToken()
    {
        return accessToken;
    }

    @Override
    public Map<String, String> getProperties()
    {
        // saphana OAuth mapping: user = empty, password = access token
        return Map.of(
            CredentialsConstants.USER, "",
            CredentialsConstants.PASSWORD, accessToken
        );
    }
}
