package com.amazonaws.athena.connector.lambda.data;

public interface BlockAllocatorRegistry
{
    BlockAllocator getOrCreateAllocator(String id);
}
