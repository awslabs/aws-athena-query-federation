package com.amazonaws.athena.connector.lambda.security;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FederatedIdentity
{
    public final String id;
    public final String principal;
    public final String account;

    @JsonCreator
    public FederatedIdentity(@JsonProperty("id") String id,
            @JsonProperty("principal") String principal,
            @JsonProperty("account") String account)
    {
        this.id = id;
        this.principal = principal;
        this.account = account;
    }

    @JsonProperty("id")
    public String getId()
    {
        return id;
    }

    @JsonProperty("principal")
    public String getPrincipal()
    {
        return principal;
    }

    @JsonProperty("account")
    public String getAccount()
    {
        return account;
    }
}
