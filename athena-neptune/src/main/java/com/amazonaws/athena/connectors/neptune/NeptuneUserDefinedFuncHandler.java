// /*-
//  * #%L
//  * athena-neptune
//  * %%
//  * Copyright (C) 2019 Amazon Web Services
//  * %%
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *      http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  * #L%
//  */
// package com.amazonaws.athena.connectors.neptune;

// import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
// import org.apache.arrow.util.VisibleForTesting;
// import org.apache.commons.codec.binary.Base64;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import javax.crypto.BadPaddingException;
// import javax.crypto.Cipher;
// import javax.crypto.IllegalBlockSizeException;
// import javax.crypto.NoSuchPaddingException;
// import javax.crypto.spec.SecretKeySpec;

// import java.security.InvalidKeyException;
// import java.security.NoSuchAlgorithmException;
// import java.util.Map;

// public class NeptuneUserDefinedFuncHandler
//         extends UserDefinedFunctionHandler
// {
//     private static final Logger logger = LoggerFactory.getLogger(NeptuneUserDefinedFuncHandler.class);

//     private static final String SOURCE_TYPE = "custom";

//     public NeptuneUserDefinedFuncHandler()
//     {
//         super(SOURCE_TYPE);
//     }

//     /**
//      * This UDF extracts an 'Account' from the input STRUCT (provided as a Map). In this case 'Account' is
//      * an application specific concept and very custom to our test dataset's schema.
//      *
//      * @param transaction The transaction from which to extract the id field.
//      * @return An Integer containing the Transaction ID or -1 if the id couldn't be extracted.
//      *
//      * @note The UserDefinedFunctionHandler class that this class extends handles mapping the UDFs we use in our
//      * SQL queries into calls to this function.
//      */
//     public Integer extract_tx_id(Map<String, Object> transaction)
//     {
//         /**
//          * TODO: Uncomment the below code that extracts the account id field from the input.
//          *
//          * 
//          * 
//          *
//          */
//          if (transaction == null || !transaction.containsKey("id")) {
//          //unknown account
//          return -1;
//          }

//          try {
//          return (Integer) transaction.get("id");
//          }
//          catch (RuntimeException ex) {
//          //We are choosing to return the default (-1) on failure but you may want to throw (which will fail your query)
//          logger.warn("extractAccount: failed to extract account.", ex);
//          }

//         return -1;
//     }

//     /**
//      * Decrypts the provided value using our application's secret key and encryption Algo.
//      *
//      * @param payload The cipher text to decrypt.
//      * @return ClearText version if the input payload, null if the decrypt failed.
//      *
//      * @note The UserDefinedFunctionHandler class that this class extends handles mapping the UDFs we use in our
//      * SQL queries into calls to this function.
//      */
//     public String decrypt(String payload)
//     {
//         String encryptionKey = null;
//         String result = null;

//         /**
//          *TODO: Uncomment the below code which retrieves our encryption key and then decrypts the
//          * secret data in our payload.
//          *
//          *
//          */
//         try {
//             encryptionKey = getEncryptionKey();
//             result = symmetricDecrypt(payload, encryptionKey);
//         }
//         catch (Exception ex) {
//             logger.warn("decrypt: failed to decrypt {}.", payload, ex);
//             //We are choosing to return null on failure but you may want to throw (which will fail your query)
//             return null;
//         }


//         return result;
//     }

//     /**
//      * This is an extremely POOR usage of AES-GCM and is only mean to illustrate how one could
//      * use a UDF for masking a field using encryption. In production scenarios we would recommend
//      * using AWS KMS for Key Management and a strong cipher like AES-GCM.
//      *
//      * @param text The text to decrypt.
//      * @param secretKey The password/key to use to decrypt the text.
//      * @return The decrypted text.
//      */
//     @VisibleForTesting
//     protected String symmetricDecrypt(String text, String secretKey)
//             throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException,
//             IllegalBlockSizeException
//     {
//         Cipher cipher;
//         String encryptedString;
//         byte[] encryptText;
//         byte[] raw;
//         SecretKeySpec skeySpec;
//         raw = Base64.decodeBase64(secretKey);
//         skeySpec = new SecretKeySpec(raw, "AES");
//         encryptText = Base64.decodeBase64(text);
//         cipher = Cipher.getInstance("AES");
//         cipher.init(Cipher.DECRYPT_MODE, skeySpec);
//         encryptedString = new String(cipher.doFinal(encryptText));
//         return encryptedString;
//     }

//     /**
//      * DO _NOT_ manage keys like this in real world usage. We are hard coding a key here to work
//      * with the tutorial's generated data set. In production scenarios we would recommend
//      * using AWS KMS for Key Management and a strong cipher like AES-GCM.
//      *
//      * @return A String representation of the AES encryption key to use to decrypt data.
//      */
//     @VisibleForTesting
//     protected String getEncryptionKey()
//     {
//         //must be exactly 24 chars or the KeySpec will fail. In general this is a poor, but simple, way to store the key.
//         return "AMzDLG4D039Km2IxIzQwfg==";
//     }
// }
