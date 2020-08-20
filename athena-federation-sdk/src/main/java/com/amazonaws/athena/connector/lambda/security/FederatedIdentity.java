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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Defines the identity of the Athena caller. This is used in many of the SDK's request objects to convey to your
 * connector or UDF the identity of the caller that triggered the subsequent Lambda invocation.
 */
public class FederatedIdentity
{
    private final String arn;
    private final String account;
    private final Map<String, String> principalTags;
    private final List<String> iamGroups;

    @JsonCreator
    public FederatedIdentity(@JsonProperty("arn") String arn,
                             @JsonProperty("account") String account,
                             @JsonProperty("principalTags") Map<String, String> principalTags,
                             @JsonProperty("iamGroups") List<String> iamGroups)
    {
        this.arn = arn;
        this.account = account;
        this.principalTags = principalTags;
        this.iamGroups = iamGroups;
    }

    @JsonProperty("arn")
    public String getArn()
    {
        return arn;
    }

    @JsonProperty("account")
    public String getAccount()
    {
        return account;
    }

    @JsonProperty("principalTags")
    public Map<String, String> getPrincipalTags()
    {
        return principalTags;
    }

    @JsonProperty("iamGroups")
    public List<String> getIamGroups()
    {
        return iamGroups;
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

        FederatedIdentity that = (FederatedIdentity) o;
        return arn.equals(that.arn) &&
                       account.equals(that.account) &&
                       principalTags.equals(that.principalTags) &&
                       iamGroups.equals(that.iamGroups);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arn, account, principalTags, iamGroups);
    }
}
