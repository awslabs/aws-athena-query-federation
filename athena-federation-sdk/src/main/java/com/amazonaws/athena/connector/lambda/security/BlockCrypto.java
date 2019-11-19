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
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Defines a facility that can be used to encrypt and decrypt blocks.
 */
public interface BlockCrypto
{
    /**
     * Used to encrypt the provided Block in its serialized form.
     *
     * @param key The EncryptionKey to use when encrypting the Block.
     * @param block The Block to serialize and encrypt.
     * @return The encrypted byte[] representation of the serialized Block, excluding its Schema.
     */
    byte[] encrypt(EncryptionKey key, Block block);

    /**
     * Used to decrypt and deserialize a Block from the provided bytes and schema.
     *
     * @param key The EncryptionKey to use when decrypting the Block.
     * @param bytes The encrypted serialized form of the Block.
     * @param schema The schema of the encrypted block
     * @return The Block.
     */
    Block decrypt(EncryptionKey key, byte[] bytes, Schema schema);

    /**
     * Used to decrypt a Block's serialzied form.
     *
     * @param key The EncryptionKey to use when decrypting the Block.
     * @param bytes The encrypted serialized form of the Block.
     * @return The serialzied Block.
     * @note This is helpful when you want to decouple encryption from Arrow processing.
     */
    byte[] decrypt(EncryptionKey key, byte[] bytes);
}
