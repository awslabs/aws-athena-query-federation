package com.amazonaws.athena.connector.lambda.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Arrays;

public class EncryptionKey
{
    private final byte[] key;
    private final byte[] nonce;

    @JsonCreator
    public EncryptionKey(@JsonProperty("key") byte[] key, @JsonProperty("nonce") byte[] nonce)
    {
        this.key = key;
        this.nonce = nonce;
    }

    @JsonProperty
    public byte[] getKey()
    {
        return key;
    }

    @JsonProperty
    public byte[] getNonce()
    {
        return nonce;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        EncryptionKey that = (EncryptionKey) o;

        return Arrays.equals(this.key, that.key) &&
                Arrays.equals(this.nonce, that.nonce);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(key) + 31 + Arrays.hashCode(nonce);
    }
}
