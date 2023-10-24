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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.example.ExampleUserDefinedFuncHandler.GCM_IV_LENGTH;
import static org.junit.Assert.*;

public class ExampleUserDefinedFuncHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleUserDefinedFuncHandlerTest.class);

    private ExampleUserDefinedFuncHandler handler = new ExampleUserDefinedFuncHandler();
    private boolean enableTests = System.getenv("publishing") != null &&
            System.getenv("publishing").equalsIgnoreCase("true");

    @Test
    public void extractTxId()
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail.
            //This is how we avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("extractAccount: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }
        Map<String, Object> input = new HashMap<>();
        input.put("id", 1000);
        assertEquals(1000, (int) handler.extract_tx_id(input));
    }

    @Test
    public void decrypt()
    {
        if (!enableTests) {
            //We do this because until you complete the tutorial these tests will fail.
            //This is how we avoid breaking the build but still have a useful tutorial. We are also duplicateing this block
            //on purpose since this is a somewhat odd pattern.
            logger.info("extractAccount: Tests are disabled, to enable them set the 'publishing' environment variable " +
                    "using maven clean install -Dpublishing=true");
            return;
        }

        String encryptedValue = symmetricEncrypt("SecretText-1755604178", handler.getEncryptionKey());
        assertTrue(handler.decrypt(encryptedValue).equals("SecretText-1755604178"));
    }

    @Test
    public void testEncryption()
            throws IOException, InvalidParameterSpecException, BadPaddingException, IllegalBlockSizeException,
            NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException,
            InvalidAlgorithmParameterException
    {
        String key = handler.getEncryptionKey();
        String value = "myValue";
        String encrypted = symmetricEncrypt(value, key);
        String actual = handler.symmetricDecrypt(encrypted, key);
        assertEquals(value, actual);
        //TODO: find and test the sample_data file automatically
        //NOTE!!!!!! _______IF_THIS_REQUIRES_A_CHANGE_THEN_YOU_NEED_TO_UPDATE_THE_SAMPLE_DATA.CSV___________
        String encryptedValue = symmetricEncrypt("SecretText-1755604178", key);
        assertTrue(handler.symmetricDecrypt(encryptedValue, key).equals("SecretText-1755604178"));
    }

    /**
     * Used to test the decrypt function in the handler.
     * This example is taken from the UDF handle example
     */
    private static String symmetricEncrypt(String plaintext, String secretKey)
    {
        try {
            byte[] plaintextKey = Base64.getDecoder().decode(secretKey);
            Cipher cipher = ExampleUserDefinedFuncHandler.getCipher(Cipher.ENCRYPT_MODE, plaintextKey, ExampleUserDefinedFuncHandler.getGCMSpecEncryption());
            byte[] encryptedContent = cipher.doFinal(plaintext.getBytes());
            // prepend ciphertext with IV
            ByteBuffer byteBuffer = ByteBuffer.allocate(GCM_IV_LENGTH + encryptedContent.length);
            byteBuffer.put(cipher.getIV());
            byteBuffer.put(encryptedContent);

            byte[] encodedContent = Base64.getEncoder().encode(byteBuffer.array());
            return new String(encodedContent);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }
}
