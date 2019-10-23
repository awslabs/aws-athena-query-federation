package com.amazonaws.athena.connector.lambda.security;

public class IdentityUtil
{
    private IdentityUtil() {}

    public static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("access_key_id", "principle", "account");
    }
}
