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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.RecordBatchSerDe;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;

/**
 * Implementation of BlockCrypto which uses AES-GCM for encrypting and decrypting blocks.
 *
 * @see BlockCrypto
 */
public class AesGcmBlockCrypto
        implements BlockCrypto
{
    protected static final int GCM_TAG_LENGTH_BITS = 16 * 8;
    protected static final int NONCE_BYTES = 12;
    protected static final int KEY_BYTES = 16;
    protected static final String KEYSPEC = "AES";
    protected static final String ALGO = "AES/GCM/NoPadding";
    protected static final String ALGO_BC = "BC";

    private final RecordBatchSerDe serDe;
    private final BlockAllocator allocator;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public AesGcmBlockCrypto(BlockAllocator allocator)
    {
        this.serDe = new RecordBatchSerDe(allocator);
        this.allocator = allocator;
    }

    public byte[] encrypt(EncryptionKey key, Block block)
    {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            serDe.serialize(block.getRecordBatch(), out);

            Cipher cipher = makeCipher(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(out.toByteArray());
        }
        catch (BadPaddingException | IllegalBlockSizeException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Block decrypt(EncryptionKey key, byte[] bytes, Schema schema)
    {
        try {
            Cipher cipher = makeCipher(Cipher.DECRYPT_MODE, key);
            byte[] clear = cipher.doFinal(bytes);

            Block resultBlock = allocator.createBlock(schema);
            resultBlock.loadRecordBatch(serDe.deserialize(clear));

            return resultBlock;
        }
        catch (BadPaddingException | IllegalBlockSizeException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] decrypt(EncryptionKey key, byte[] bytes)
    {
        try {
            Cipher cipher = makeCipher(Cipher.DECRYPT_MODE, key);
            return cipher.doFinal(bytes);
        }
        catch (BadPaddingException | IllegalBlockSizeException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Cipher makeCipher(int mode, EncryptionKey key)
    {
        if (key.getNonce().length != NONCE_BYTES) {
            throw new RuntimeException("Expected " + NONCE_BYTES + " nonce bytes but found " + key.getNonce().length);
        }

        if (key.getKey().length != KEY_BYTES) {
            throw new RuntimeException("Expected " + KEY_BYTES + " key bytes but found " + key.getKey().length);
        }

        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, key.getNonce());
        SecretKeySpec secretKeySpec = new SecretKeySpec(key.getKey(), KEYSPEC);

        try {
            Cipher cipher = Cipher.getInstance(ALGO, ALGO_BC);
            cipher.init(mode, secretKeySpec, spec);
            return cipher;
        }
        catch (NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException
                | NoSuchProviderException | NoSuchPaddingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
