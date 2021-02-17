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

import org.apache.commons.lang3.Validate;

/**
 * Holds the credentials retrieved from SecretsManager.
 */
public class SecretsManagerCredentials
{
    private final String secretName;
    private final String username;
    private final String password;
    private final String arn;

    public SecretsManagerCredentials(String secretName, String username, String password, String arn)
    {
        this.secretName = Validate.notNull(secretName, "secretName is null.");
        this.username = Validate.notNull(username, "username is null.");
        this.password = Validate.notNull(password, "password is null.");
        this.arn = Validate.notNull(arn, "arn is null.");
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

    /**
     * Public accessor to the arn associated with the secret retrieved from SecretsManager.
     * @return Arn (String).
     */
    public String getArn()
    {
        return arn;
    }
}
