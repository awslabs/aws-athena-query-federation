/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.integ.data;

/**
 * Holds the credentials retrieved from SecretsManager.
 */
public class SecretsManagerCredentials
{
    private final String secretName;
    private final String username;
    private final String password;

    public SecretsManagerCredentials(String secretName, String username, String password)
    {
        this.secretName = secretName;
        this.username = username;
        this.password = password;
    }

    /**
     * Public accessor to the secret name used to retrieve the credentials from SecretsManager.
     * @return Secret name (String).
     */
    public String getSecretName()
    {
        return secretName;
    }

    /**
     * Public accessor to the username retrieved from SecretsManager.
     * @return Username (String).
     */
    public String getUsername()
    {
        return username;
    }

    /**
     * Public accessor to the password retrieved from SecretsManager.
     * @return Password (String).
     */
    public String getPassword()
    {
        return password;
    }
}
