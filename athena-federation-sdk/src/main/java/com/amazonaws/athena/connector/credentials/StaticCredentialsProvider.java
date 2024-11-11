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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

/**
 * Static credential provider.
 */
public class StaticCredentialsProvider
        implements CredentialsProvider
{
    private final DefaultCredentials defaultCredentials;

    /**
     * @param defaultCredentials JDBC credential. See {@link DefaultCredentials}.
     */
    public StaticCredentialsProvider(final DefaultCredentials defaultCredentials)
    {
        this.defaultCredentials = Validate.notNull(defaultCredentials, "jdbcCredential must not be null.");

        if (StringUtils.isAnyBlank(jdbcCredential.getUser(), jdbcCredential.getPassword())) {
            throw new AthenaConnectorException("User or password must not be blank.",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
    }

    @Override
    public DefaultCredentials getCredential()
    {
        return this.defaultCredentials;
    }
}
