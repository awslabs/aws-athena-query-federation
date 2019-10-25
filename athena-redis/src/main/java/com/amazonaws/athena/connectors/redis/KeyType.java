package com.amazonaws.athena.connectors.redis;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines the support key types that can be used to define the keys that comprise a Redis table in glue.
 */
public enum KeyType
{
    /**
     * Indicates that the KeyType is a prefix and so all Redis keys matching this prefix are in scope for the Table.
     */
    PREFIX("prefix"),

    /**
     * Indicates that the KeyType is a zset and so all Keys that match the value with be zsets and as such we
     * should take all the values in those keys and treat them as keys that are in scope for the Table.
     *
     * For example: my_key_list is a a key which points to a zset that contains: key1, key2, key3. So when I query
     *              this table. We lookup my_key_list and for each value (key1, key2, key3) in that zset we lookup
     *              the value. So our table contains the values stored at key1, key2, key3.
     */
    ZSET("zset");

    private static final Map<String, KeyType> TYPE_MAP = new HashMap<>();

    static {
        for (KeyType next : KeyType.values()) {
            TYPE_MAP.put(next.id, next);
        }
    }

    private String id;

    KeyType(String id)
    {
        this.id = id;
    }

    public String getId()
    {
        return id;
    }

    public static KeyType fromId(String id)
    {
        KeyType result = TYPE_MAP.get(id);
        if (result == null) {
            throw new IllegalArgumentException("Unknown KeyType for id: " + id);
        }

        return result;
    }
}
