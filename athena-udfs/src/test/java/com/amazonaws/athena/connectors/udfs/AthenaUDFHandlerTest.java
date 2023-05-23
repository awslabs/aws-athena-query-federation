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

import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;
import java.util.zip.DataFormatException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AthenaUDFHandlerTest
{
    private static final String DUMMY_SECRET_NAME = "dummy_secret";

    private AthenaUDFHandler athenaUDFHandler;

    private static final String PLAINTEXT_DATA_KEY = "AQIDBAUGBwgJAAECAwQFBg==";

    private Base64.Decoder decoder = Base64.getDecoder();
    private Base64.Encoder encoder = Base64.getEncoder();

    @Before
    public void setup()
    {
        CachableSecretsManager cachableSecretsManager = mock(CachableSecretsManager.class);
        when(cachableSecretsManager.getSecret(DUMMY_SECRET_NAME)).thenReturn(PLAINTEXT_DATA_KEY);
        this.athenaUDFHandler = new AthenaUDFHandler(cachableSecretsManager);
    }

    @Test
    public void testCompressAndDecompressHappyCase()
    {
        String input = "StringToBeCompressed";

        String compressed = athenaUDFHandler.compress(input);
        assertEquals("eJwLLinKzEsPyXdKdc7PLShKLS5OTQEAUrEH9w==", compressed);

        String decompressed = athenaUDFHandler.decompress(compressed);
        assertEquals(input, decompressed);
    }

    @Test
    public void testCompressNull()
    {
        assertNull(athenaUDFHandler.compress(null));
    }

    @Test
    public void testDecompressNull()
    {
        assertNull(athenaUDFHandler.decompress(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressNonCompressedInput()
    {
        athenaUDFHandler.decompress("jklasdfkljsadflkafdsjklsdfakljadsfkjldaadfasdffsa");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressBadInputEncoding()
    {
        athenaUDFHandler.decompress("78 da 0b c9 cf ab 54 70 cd 49 2d 4b 2c");
    }

    @Test
    public void testDecompressTruncatedInput()
    {
        try {
            athenaUDFHandler.decompress("");
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof DataFormatException);
            assertEquals("Input is truncated", e.getCause().getMessage());
        }
    }

    @Test
    public void testKmsDecryption() throws Exception
    {
        SecretKeySpec skeySpec = new SecretKeySpec(decoder.decode(PLAINTEXT_DATA_KEY), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

        String expected = "abcdef";
        String encryptedString = new String(encoder.encode(cipher.doFinal(expected.getBytes())));

        String result = athenaUDFHandler.decrypt(encryptedString, DUMMY_SECRET_NAME);

        assertEquals(expected, result);
    }

    @Test
    public void testKmsEncryption() throws Exception
    {
        SecretKeySpec skeySpec = new SecretKeySpec(decoder.decode(PLAINTEXT_DATA_KEY), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

        String content = "abcdef";
        String expected = new String(encoder.encode(cipher.doFinal(content.getBytes())));

        String result = athenaUDFHandler.encrypt(content, DUMMY_SECRET_NAME);

        assertEquals(expected, result);
    }

    @Test
    public void testKmsEncryptionNull() {
        assertNull(athenaUDFHandler.encrypt(null, DUMMY_SECRET_NAME));
    }

    @Test
    public void testKmsDecryptionNull() {
        assertNull(athenaUDFHandler.decrypt(null, DUMMY_SECRET_NAME));
    }

    /**
     * This UT is used to test {@link AthenaUDFHandler#decrypt(String, String)} method end-to-end.
     * It requires AWS Secret Manager setup and AWS credential setup.
     * @throws Exception
     */
    @Ignore("Enabled as needed to do end-to-end test")
    @Test
    public void testKmsDecryptionEndToEnd() throws Exception
    {
        String secretName = "<fill-in-your-secret-name>";
        String secretValue = "<fill-in-secret-value>";

        this.athenaUDFHandler = new AthenaUDFHandler();

        SecretKeySpec skeySpec = new SecretKeySpec(decoder.decode(secretValue), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

        String expected = "abcdef";
        String encryptedString = new String(encoder.encode(cipher.doFinal(expected.getBytes())));

        String result = athenaUDFHandler.decrypt(encryptedString, secretName);

        assertEquals(expected, result);
    }

    /**
     * This UT is used to test {@link AthenaUDFHandler#encrypt(String, String)} method end-to-end.
     * It requires AWS Secret Manager setup and AWS credential setup.
     * @throws Exception
     */
    @Ignore("Enabled as needed to do end-to-end test")
    @Test
    public void testKmsEncryptionEndToEnd() throws Exception
    {
        String secretName = "<fill-in-your-secret-name>";
        String secretValue = "<fill-in-secret-value>";

        this.athenaUDFHandler = new AthenaUDFHandler();

        SecretKeySpec skeySpec = new SecretKeySpec(decoder.decode(secretValue), "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

        String expected = "abcdef";
        String encryptedString = new String(encoder.encode(cipher.doFinal(expected.getBytes())));

        String result = athenaUDFHandler.decrypt(encryptedString, secretName);

        assertEquals(expected, result);
    }
}
