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
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.google.common.annotations.VisibleForTesting;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

    private final AWSKMS kms;
    private final SecretKeySpec skeySpec;

    public AthenaUDFHandler()
    {
        this(AWSKMSClientBuilder.standard().build());
    }

    public AthenaUDFHandler(AWSKMS kms)
    {
        super(SOURCE_TYPE);

        this.kms = kms;
        byte[] encryptedDataKey = getEncryptedDataKey(kms);
        DecryptRequest decryptRequest = new DecryptRequest()
                .withCiphertextBlob(ByteBuffer.wrap(encryptedDataKey));
        byte[] plainTextDataKey = kms.decrypt(decryptRequest).getPlaintext().array();
        skeySpec = new SecretKeySpec(plainTextDataKey, "AES");
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
     * This method decrypts the ciphertext with a data key stored in this Lambda function's memory. The data key is
     * acquired by {@link #getEncryptedDataKey(AWSKMS)} method and converted to plaintext in AthenaUDFHandler's
     * consturctor. The plaintext data key is stored in the skeySpec object. Any subsequent invocation of
     * {@link #decrypt(String)} method would create a cipher and decrypt the text with the plaintext data key acquired
     * above.
     *
     * To use this function, finish the "TODO" by setting the KMS key id in {@link #getEncryptedDataKey(AWSKMS)}. Also
     * make sure your Lambda function is allowed to access your KMS key. 
     *
     * @param cipherText cipher text encoded with base64
     * @return plaintext data
     */
    public String decrypt(String cipherText)
    {
        try {
            Cipher cipher = getCipher();
            byte[] encryptedContent = Base64.getDecoder().decode(cipherText.getBytes());
            byte[] plainTextBytes = cipher.doFinal(encryptedContent);
            return new String(plainTextBytes);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    private Cipher getCipher()
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            return cipher;
        }
        catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The plaintext data key is hardcoded here for demo purpose. Do _NOT_ do this in practice. You should create your
     * own data key in a safe place, encrypt them and store them properly.
     */
    @VisibleForTesting
    static final byte[] plainTextDataKeyForSetup
            = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};

    /**
     * For demo purpose, we generate a new data key here. In practice, this method should download a encrypted data
     * key from a safe and durable store, e.g. S3 or DynamoDB with encryption enabled.
     *
     * @return encrypted data key
     */
    @VisibleForTesting
    byte[] getEncryptedDataKey(AWSKMS kms)
    {
        //todo: set the keyId to a KMS key id you own.
        String keyId = "<KMS key id you own>";

        ByteBuffer plaintext
                = ByteBuffer.wrap(plainTextDataKeyForSetup);
        EncryptRequest req = new EncryptRequest().withKeyId(keyId).withPlaintext(plaintext);
        ByteBuffer ciphertext = kms.encrypt(req).getCiphertextBlob();
        return ciphertext.array();
    }
}
