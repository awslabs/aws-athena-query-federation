/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Streaming iterator for DynamoDB items that processes data as it arrives
 */
public class StreamingIterator implements Iterator<Map<String, AttributeValue>>
{
    private final BlockingQueue<Map<String, AttributeValue>> buffer;
    private final int bufferSize;
    private volatile boolean finished = false;
    private Map<String, AttributeValue> nextItem;

    public StreamingIterator(int bufferSize)
    {
        this.bufferSize = bufferSize;
        this.buffer = new LinkedBlockingQueue<>(bufferSize);
    }

    public void addItem(Map<String, AttributeValue> item)
    {
        if (!finished) {
            try {
                buffer.put(item);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                finished = true;
            }
        }
    }

    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean hasNext()
    {
        if (nextItem != null) {
            return true;
        }
        
        if (finished && buffer.isEmpty()) {
            return false;
        }
        
        // Non-blocking poll for better performance
        nextItem = buffer.poll();
        if (nextItem != null) {
            return true;
        }
        
        // Only block if not finished
        if (!finished) {
            try {
                nextItem = buffer.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        return nextItem != null;
    }

    @Override
    public Map<String, AttributeValue> next()
    {
        if (nextItem != null) {
            Map<String, AttributeValue> item = nextItem;
            nextItem = null;
            return item;
        }
        
        try {
            return buffer.take();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
