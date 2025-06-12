/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.security;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NoOpBlockCryptoTest {
    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private BlockAllocator allocator;
    private NoOpBlockCrypto noOpBlockCrypto;
    private Schema testSchema;
    private EncryptionKey encryptionKey;

    @Before
    public void setup() {
        allocator = new BlockAllocatorImpl();
        noOpBlockCrypto = new NoOpBlockCrypto(allocator);
        testSchema = new Schema(Collections.emptyList());
        encryptionKey = keyFactory.create();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void encryptWithNullKey() {
        Block block = allocator.createBlock(testSchema);

        byte[] encrypted = noOpBlockCrypto.encrypt(null, block);

        assertNotNull(encrypted);
        assertTrue(encrypted.length > 0);
    }

    @Test(expected = AthenaConnectorException.class)
    public void encryptWithNonNullKeyThrowsException() {
        Block block = allocator.createBlock(testSchema);

        noOpBlockCrypto.encrypt(encryptionKey, block);
    }

    @Test
    public void decryptBytesPassthrough() {
        byte[] inputBytes = new byte[]{1, 2, 3, 4, 5};

        byte[] result = noOpBlockCrypto.decrypt(null, inputBytes);

        assertArrayEquals(inputBytes, result);
    }

    @Test
    public void decryptWithSchema() {
        // Define a simple schema with one int field
        Field intField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Collections.singletonList(intField));

        // Create a Block with this schema
        Block block = allocator.createBlock(schema);

        // Get vector and set a value
        IntVector intVector = (IntVector) block.getFieldVector("id");
        intVector.setSafe(0, 42);
        intVector.setValueCount(1);
        block.setRowCount(1);

        // Encrypt and decrypt
        byte[] encrypted = noOpBlockCrypto.encrypt(null, block);
        Block decryptedBlock = noOpBlockCrypto.decrypt(null, encrypted, schema);

        // Validate schema and data
        assertNotNull(decryptedBlock);
        assertEquals(schema, decryptedBlock.getSchema());
        IntVector resultVector = (IntVector) decryptedBlock.getFieldVector("id");
        assertEquals(1, resultVector.getValueCount());
        assertEquals(42, resultVector.get(0));
    }

    @Test(expected = AthenaConnectorException.class)
    public void decryptWithCorruptedBytesThrowsException() {
        // Create a schema (could be anything since the input is invalid)
        Field intField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Collections.singletonList(intField));

        // Pass in garbage bytes that are not valid Arrow record batches
        byte[] corruptedBytes = new byte[] { 0x01, 0x02, 0x03, 0x04 };

        // This should throw AthenaConnectorException due to IOException internally
        noOpBlockCrypto.decrypt(null, corruptedBytes, schema);
    }

}

