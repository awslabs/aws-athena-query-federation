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

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptResult;
import com.amazonaws.services.kms.model.EncryptResult;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;

import static com.amazonaws.athena.connectors.udfs.AthenaUDFHandler.plainTextDataKeyForSetup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AthenaUDFHandlerTest
{
    private AthenaUDFHandler athenaUDFHandler;

    @Before
    public void setup()
    {
        AWSKMS kms = mock(AWSKMS.class);
        byte[] dummyData = new byte[]{1,2,3,4,5};
        when(kms.encrypt(any())).thenReturn(new EncryptResult().withCiphertextBlob(ByteBuffer.wrap(dummyData)));
        when(kms.decrypt(any())).thenReturn(new DecryptResult().withPlaintext(ByteBuffer.wrap(plainTextDataKeyForSetup)));
        this.athenaUDFHandler = new AthenaUDFHandler(kms);
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

    @Test(expected = NullPointerException.class)
    public void testCompressNull()
    {
        athenaUDFHandler.compress(null);
    }

    @Test(expected = NullPointerException.class)
    public void testDecompressNull()
    {
        athenaUDFHandler.decompress(null);
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
    public void testKmsEncryption() throws Exception
    {
        SecretKeySpec skeySpec = new SecretKeySpec(plainTextDataKeyForSetup, "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

        String expected = "abcdef";
        String encryptedString = new String(Base64.encodeBase64(cipher.doFinal(expected.getBytes())));

        String result = athenaUDFHandler.decrypt(encryptedString);

        assertEquals(expected, result);
    }

    /**
     * This UT is used to test {@link AthenaUDFHandler#decrypt(String)} method end-to-end.
     * It requires KMS key setup and credential setup.
     * @throws Exception
     */
    @Ignore("Enabled as needed to do end-to-end test")
    @Test
    public void testKmsEncryptionEndToEnd() throws Exception
    {
        AWSKMS kms = AWSKMSClientBuilder.standard().build();
        this.athenaUDFHandler = new AthenaUDFHandler(kms);

        SecretKeySpec skeySpec = new SecretKeySpec(plainTextDataKeyForSetup, "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec);

        String expected = "abcdef";
        String encryptedString = new String(Base64.encodeBase64(cipher.doFinal(expected.getBytes())));

        String result = athenaUDFHandler.decrypt(encryptedString);

        assertEquals(expected, result);
    }

}
