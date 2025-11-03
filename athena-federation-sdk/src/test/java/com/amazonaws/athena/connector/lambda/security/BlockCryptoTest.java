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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockCryptoTest
{
    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private BlockAllocatorImpl allocator;
    private AesGcmBlockCrypto crypto;
    private EncryptionKey key;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        crypto = new AesGcmBlockCrypto(new BlockAllocatorImpl());
        key = keyFactory.create();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void test()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .build();

        Block expected = allocator.createBlock(schema);
        BlockUtils.setValue(expected.getFieldVector("col1"), 1, 100);
        BlockUtils.setValue(expected.getFieldVector("col2"), 1, "VarChar");
        BlockUtils.setValue(expected.getFieldVector("col1"), 1, 101);
        BlockUtils.setValue(expected.getFieldVector("col2"), 1, "VarChar1");
        expected.setRowCount(2);

        byte[] cypher = crypto.encrypt(key, expected);
        Block actual = crypto.decrypt(key, cypher, schema);
        assertEquals(expected, actual);
    }

    @Test
    public void decryptWithInvalidBytes()
    {
        EncryptionKey key = keyFactory.create();
        byte[] invalidBytes = new byte[16]; // Not valid cipher text

        try {
            crypto.decrypt(key, invalidBytes);
            fail("Expected AthenaConnectorException was not thrown");
        } catch (AthenaConnectorException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("mac check in gcm failed"));
        }
    }

    @Test
    public void makeCipherWithInvalidNonce()
    {
        try {
            byte[] badNonce = new byte[5]; // should be 12 bytes
            byte[] validKey = new byte[32]; // valid AES-256 key
            EncryptionKey invalidKey = new EncryptionKey(validKey, badNonce);
            crypto.decrypt(invalidKey, new byte[16]);
            fail("Expected AthenaConnectorException due to invalid nonce");
        } catch (AthenaConnectorException ex) {
            assertTrue(ex.getMessage().contains("Expected 12 nonce bytes"));
        }
    }

    @Test
    public void makeCipherWithEmptyKey()
    {
        try {
            byte[] emptyKey = new byte[0];
            byte[] validNonce = new byte[12];
            EncryptionKey invalidKey = new EncryptionKey(emptyKey, validNonce);
            crypto.decrypt(invalidKey, new byte[16]);
            fail("Expected AthenaConnectorException due to empty key");
        } catch (AthenaConnectorException ex) {
            assertTrue(ex.getMessage().contains("Invalid key"));
        }
    }
}
