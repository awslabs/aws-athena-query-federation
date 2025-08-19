/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.sun.jna.platform.unix.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Base64;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SSL_CERT_FILE_LOCATION;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SSL_CERT_FILE_LOCATION_VALUE;

public class SnowflakeUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeUtils.class);
    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private SnowflakeUtils()
    {
    }
    /**
     * Write out the cacerts that we trust from the default java truststore.
     *
     */
    public static void installCaCertificate() throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateEncodingException
    {
        FileWriter caBundleWriter = new FileWriter(SSL_CERT_FILE_LOCATION_VALUE);
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
                X509TrustManager x509TrustManager = (X509TrustManager) trustManager;
                for (X509Certificate x509Certificate : x509TrustManager.getAcceptedIssuers()) {
                    caBundleWriter.write(formatCrtFileContents(x509Certificate));
                    caBundleWriter.write(LINE_SEPARATOR);
                }
            }
        }
        finally {
            caBundleWriter.close();
        }
    }

    private static String formatCrtFileContents(Certificate certificate) throws CertificateEncodingException
    {
        Base64.Encoder encoder = Base64.getMimeEncoder(64, LINE_SEPARATOR.getBytes());
        byte[] rawCrtText = certificate.getEncoded();
        String encodedCertText = new String(encoder.encode(rawCrtText));
        String prettifiedCert = BEGIN_CERT + LINE_SEPARATOR + encodedCertText + LINE_SEPARATOR + END_CERT;
        return prettifiedCert;
    }

    public static void setupNativeEnvironmentVariables()
    {
        LibC.INSTANCE.setenv(SSL_CERT_FILE_LOCATION, SSL_CERT_FILE_LOCATION_VALUE, 1);
        if (logger.isDebugEnabled()) {
            logger.debug("Set native environment variables: {}: {}",
                    SSL_CERT_FILE_LOCATION, LibC.INSTANCE.getenv(SSL_CERT_FILE_LOCATION));
        }
    }
}
