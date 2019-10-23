package com.amazonaws.athena.connector.lambda.security;

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
