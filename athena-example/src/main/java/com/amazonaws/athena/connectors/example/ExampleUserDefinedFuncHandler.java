/*-
 * #%L
 * athena-example
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
package com.amazonaws.athena.connectors.example;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Base64;

public class ExampleUserDefinedFuncHandler
        extends UserDefinedFunctionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleUserDefinedFuncHandler.class);

    private static final String SOURCE_TYPE = "custom";
    public static final int GCM_IV_LENGTH = 12;
    public static final int GCM_TAG_LENGTH = 16;

    public ExampleUserDefinedFuncHandler()
    {
        super(SOURCE_TYPE);
    }

    /**
     * This UDF extracts an 'Account' from the input STRUCT (provided as a Map). In this case 'Account' is
     * an application specific concept and very custom to our test dataset's schema.
     *
     * @param transaction The transaction from which to extract the id field.
     * @return An Integer containing the Transaction ID or -1 if the id couldn't be extracted.
     *
     * @note The UserDefinedFunctionHandler class that this class extends handles mapping the UDFs we use in our
     * SQL queries into calls to this function.
     */
    public Integer extract_tx_id(Map<String, Object> transaction)
    {
        /**
         * TODO: Uncomment the below code that extracts the account id field from the input.
         *
         if (transaction == null || !transaction.containsKey("id")) {
         //unknown account
         return -1;
         }

         try {
         return (Integer) transaction.get("id");
         }
         catch (RuntimeException ex) {
         //We are choosing to return the default (-1) on failure but you may want to throw (which will fail your query)
         logger.warn("extractAccount: failed to extract account.", ex);
         }

         *
         */
        return -1;
    }

    /**
     * Decrypts the provided value using our application's secret key and encryption Algo.
     *
     * @param payload The cipher text to decrypt.
     * @return ClearText version if the input payload, null if the decrypt failed.
     *
     * @note The UserDefinedFunctionHandler class that this class extends handles mapping the UDFs we use in our
     * SQL queries into calls to this function.
     */
    public String decrypt(String payload)
    {
        String encryptionKey = null;
        String result = null;

        /**
         *TODO: Uncomment the below code which retrieves our encryption key and then decrypts the
         * secret data in our payload.
         *
        try {
            encryptionKey = getEncryptionKey();
            result = symmetricDecrypt(payload, encryptionKey);
        }
        catch (Exception ex) {
            logger.warn("decrypt: failed to decrypt {}.", payload, ex);
            //We are choosing to return null on failure but you may want to throw (which will fail your query)
            return null;
        }
         *
         */

        return result;
    }

    /**
     * This usage of AES-GCM and is only meant to illustrate how one could
     * use a UDF for masking a field using encryption. In production scenarios we would recommend
     * using AWS KMS for Key Management and a strong cipher like AES-GCM.
     *
     * @param ciphertext The text to decrypt.
     * @param secretKey The password/key to use to decrypt the text.
     * @return The decrypted text.
     */
    @VisibleForTesting
    public String symmetricDecrypt(String ciphertext, String secretKey)
    {
        if (ciphertext == null) {
            return null;
        }
        byte[] plaintextKey = Base64.getDecoder().decode(secretKey);

        try {
            byte[] encryptedContent = Base64.getDecoder().decode(ciphertext.getBytes());
            // extract IV from first GCM_IV_LENGTH bytes of ciphertext
            Cipher cipher = getCipher(Cipher.DECRYPT_MODE, plaintextKey, getGCMSpecDecryption(encryptedContent));
            byte[] plainTextBytes = cipher.doFinal(encryptedContent, GCM_IV_LENGTH, encryptedContent.length - GCM_IV_LENGTH);
            return new String(plainTextBytes);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    private static GCMParameterSpec getGCMSpecDecryption(byte[] encryptedText)
    {
        return new GCMParameterSpec(GCM_TAG_LENGTH * Byte.SIZE, encryptedText, 0, GCM_IV_LENGTH);
    }

    static GCMParameterSpec getGCMSpecEncryption()
    {
        byte[] iv = new byte[GCM_IV_LENGTH];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);

        return new GCMParameterSpec(GCM_TAG_LENGTH * Byte.SIZE, iv);
    }

    static Cipher getCipher(int cipherMode, byte[] plainTextDataKey, GCMParameterSpec gcmParameterSpec)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES_256/GCM/NoPadding");
            SecretKeySpec skeySpec = new SecretKeySpec(plainTextDataKey, "AES");

            cipher.init(cipherMode, skeySpec, gcmParameterSpec);
            return cipher;
        }
        catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException |
               InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * DO _NOT_ manage keys like this in real world usage. We are hard coding a key here to work
     * with the tutorial's generated data set. In production scenarios we would recommend
     * using AWS KMS for Key Management and a strong cipher like AES-GCM.
     *
     * @return A String representation of the AES encryption key to use to decrypt data.
     */
    @VisibleForTesting
    protected String getEncryptionKey()
    {
        // The algorithm used requires 32 Byte Key! 
        // Can be generated for testing using `openssl rand -base64 32`
        return "i5YnyBO4gJKWuIQ+gjuJjcJ/5kUph9pmYFUbW7zf3PE=";
    }
}
