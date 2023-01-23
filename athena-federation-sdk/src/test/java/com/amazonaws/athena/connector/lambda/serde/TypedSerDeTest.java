/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.utils.TestUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.utils.TestUtils.SERDE_VERSION_FOUR;
import static com.amazonaws.athena.connector.lambda.utils.TestUtils.SERDE_VERSION_TWO;

public abstract class TypedSerDeTest<T>
{
    protected TestUtils utils = new TestUtils();
    protected BlockAllocator allocator;
    protected ObjectMapper mapper;
    protected ObjectMapper mapperV4;
    protected FederatedIdentity federatedIdentity = new FederatedIdentity("testArn", "0123456789", Collections.emptyMap(), Collections.emptyList());
    protected String expectedSerDeText;
    protected T expected;

    @Before
    public void before()
    {
        allocator = new BlockAllocatorImpl("test-allocator-id");
        mapper = VersionedObjectMapperFactory.create(allocator, SERDE_VERSION_TWO);
        mapperV4 = VersionedObjectMapperFactory.create(allocator, SERDE_VERSION_FOUR);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapperV4.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @After
    public void after()
    {
        allocator.close();
    }

    public abstract void serialize()
            throws Exception;

    public abstract void deserialize()
            throws IOException;
}
