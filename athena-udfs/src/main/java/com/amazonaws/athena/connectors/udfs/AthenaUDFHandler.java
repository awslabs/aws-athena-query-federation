/*-
 * #%L
 * athena-udfs
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
package com.amazonaws.athena.connectors.udfs;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClient;
import com.google.common.annotations.VisibleForTesting;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class AthenaUDFHandler
        extends UserDefinedFunctionHandler
{
    private static final String SOURCE_TYPE = "athena_common_udfs";

    private final CachableSecretsManager cachableSecretsManager;

    public AthenaUDFHandler()
    {
        this(new CachableSecretsManager(AWSSecretsManagerClient.builder().build()));
    }

    @VisibleForTesting
    AthenaUDFHandler(CachableSecretsManager cachableSecretsManager)
    {
        super(SOURCE_TYPE);
        this.cachableSecretsManager = cachableSecretsManager;
    }

    /**
     * Compresses a valid UTF-8 String using the zlib compression library.
     * Encodes bytes with Base64 encoding scheme.
     *
     * @param input the String to be compressed
     * @return the compressed String
     */
    public String compress(String input)
    {
        if (input == null) {
            return null;
        }

        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);

        // create compressor
        Deflater compressor = new Deflater();
        compressor.setInput(inputBytes);
        compressor.finish();

        // compress bytes to output stream
        byte[] buffer = new byte[4096];
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(inputBytes.length);
        while (!compressor.finished()) {
            int bytes = compressor.deflate(buffer);
            byteArrayOutputStream.write(buffer, 0, bytes);
        }

        try {
            byteArrayOutputStream.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to close ByteArrayOutputStream", e);
        }

        // return encoded string
        byte[] compressedBytes = byteArrayOutputStream.toByteArray();
        return Base64.getEncoder().encodeToString(compressedBytes);
    }

    /**
     * Decompresses a valid String that has been compressed using the zlib compression library.
     * Decodes bytes with Base64 decoding scheme.
     *
     * @param input the String to be decompressed
     * @return the decompressed String
     */
    public String decompress(String input)
    {
        if (input == null) {
            return null;
        }

        byte[] inputBytes = Base64.getDecoder().decode((input));

        // create decompressor
        Inflater decompressor = new Inflater();
        decompressor.setInput(inputBytes, 0, inputBytes.length);

        // decompress bytes to output stream
        byte[] buffer = new byte[4096];
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(inputBytes.length);
        try {
            while (!decompressor.finished()) {
                int bytes = decompressor.inflate(buffer);
                if (bytes == 0 && decompressor.needsInput()) {
                    throw new DataFormatException("Input is truncated");
                }
                byteArrayOutputStream.write(buffer, 0, bytes);
            }
        }
        catch (DataFormatException e) {
            throw new RuntimeException("Failed to decompress string", e);
        }

        try {
            byteArrayOutputStream.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to close ByteArrayOutputStream", e);
        }

        // return decoded string
        byte[] decompressedBytes = byteArrayOutputStream.toByteArray();
        return new String(decompressedBytes, StandardCharsets.UTF_8);
    }

    /**
     * This method decrypts the ciphertext with a data key stored AWS Secret Manager. Before using this function, create
     * a secret in AWS Secret Manager. Do a base64 encode to your data key and convert it to string. Store it as
     * _PLAINTEXT_ in the secret (do not include any quotes, brackets, etc). Also make sure to use DefaultEncryptionKey
     * as the KMS key. Otherwise you would need to update athena-udfs.yaml to allow access to your KMS key.
     *
     * @param ciphertext
     * @param secretName
     * @return plaintext
     */
    public String decrypt(String ciphertext, String secretName)
    {
        if (ciphertext == null) {
            return null;
        }

        String secretString = cachableSecretsManager.getSecret(secretName);
        byte[] plaintextKey = Base64.getDecoder().decode(secretString);

        try {
            Cipher cipher = getCipher(Cipher.DECRYPT_MODE, plaintextKey);
            byte[] encryptedContent = Base64.getDecoder().decode(ciphertext.getBytes());
            byte[] plainTextBytes = cipher.doFinal(encryptedContent);
            return new String(plainTextBytes);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method encrypts the plaintext with a data key stored AWS Secret Manager. Before using this function, create
     * a secret in AWS Secret Manager. Do a base64 encode to your data key and convert it to string. Store it as
     * _PLAINTEXT_ in the secret (do not include any quotes, brackets, etc). Also make sure to use DefaultEncryptionKey
     * as the KMS key. Otherwise you would need to update athena-udfs.yaml to allow access to your KMS key.
     *
     * @param plaintext
     * @param secretName
     * @return ciphertext
     */
    public String encrypt(String plaintext, String secretName)
    {
        if (plaintext == null) {
            return null;
        }

        String secretString = cachableSecretsManager.getSecret(secretName);
        byte[] plaintextKey = Base64.getDecoder().decode(secretString);

        try {
            Cipher cipher = getCipher(Cipher.ENCRYPT_MODE, plaintextKey);
            byte[] encryptedContent = cipher.doFinal(plaintext.getBytes());
            byte[] encodedContent = Base64.getEncoder().encode(encryptedContent);
            return new String(encodedContent);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    private Cipher getCipher(int cipherMode, byte[] plainTextDataKey)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            SecretKeySpec skeySpec = new SecretKeySpec(plainTextDataKey, "AES");
            cipher.init(cipherMode, skeySpec);
            return cipher;
        }
        catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }
}
