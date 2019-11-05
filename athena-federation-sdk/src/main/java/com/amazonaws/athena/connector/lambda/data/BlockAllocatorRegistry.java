package com.amazonaws.athena.connector.lambda.data;

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

/**
 * Used to track BlockAllocators in transactional environments where you want tighter control over
 * how much memory a particular transaction uses. By using the same BlockAllocator for all resources
 * in a given transaction you can limit the total memory used by that transaction. This also proves
 * to be a useful mechanism to inject BlockAllocators in contexts which are difficult to access with
 * more traditional dependency injection mechanism. One such example is in a ObjectMapper that is deserializing
 * and incoming request.
 */
public interface BlockAllocatorRegistry
{
    /**
     * Gets or creates a new Block Allocator for the given context (id).
     *
     * @param id The id of the context for which you'd like a BlockAllocator.
     * @return The BlockAllocator associated with that id, or a new BlockAllocator for that id which will then
     * be vended for any future calls for that id.
     */
    BlockAllocator getOrCreateAllocator(String id);
}
