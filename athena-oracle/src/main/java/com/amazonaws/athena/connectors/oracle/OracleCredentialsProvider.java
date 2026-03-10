/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class OracleCredentialsProvider extends DefaultCredentialsProvider
{
    public static final String IS_FIPS_ENABLED = "is_fips_enabled";
    public static final String IS_FIPS_ENABLED_LEGACY = "is_FIPS_Enabled";
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleCredentialsProvider.class);

    private final String jdbcConnectionString;

    public OracleCredentialsProvider(final String secretString, final String jdbcConnectionString)
    {
        super(secretString);
        this.jdbcConnectionString = jdbcConnectionString;
    }

    @Override
    public Map<String, String> getCredentialMap()
    {
        DefaultCredentials creds = getCredential();
        String password = creds.getPassword();
        if (!password.contains("\"")) {
            password = String.format("\"%s\"", password);
        }
        
        Map<String, String> credMap = new HashMap<>();
        credMap.put(CredentialsConstants.USER, creds.getUser());
        credMap.put(CredentialsConstants.PASSWORD, password);

        //checking for tcps (Secure Communication) protocol as part of the connection string.
        if (jdbcConnectionString != null && jdbcConnectionString.toLowerCase().contains("@tcps://")) {
            LOGGER.info("Adding SSL properties...");
            credMap.put("javax.net.ssl.trustStoreType", "JKS");
            credMap.put("javax.net.ssl.trustStorePassword", "changeit");
            credMap.put("oracle.net.ssl_server_dn_match", "true");

            // By default; Oracle RDS uses SSL_RSA_WITH_AES_256_CBC_SHA
            // Adding the following cipher suits to support others listed in Doc
            // https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.Oracle.Options.SSL.html#Appendix.Oracle.Options.SSL.CipherSuites
            if (isFipsEnabled()) {
                credMap.put("oracle.net.ssl_cipher_suites", 
                    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384," +
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256," +
                    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384," +
                    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256," +
                    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA," +
                    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
            }
        }
        
        return credMap;
    }

    // Returns true if either current or legacy FIPS environment variable is set to "true"
    private boolean isFipsEnabled()
    {
        return Boolean.parseBoolean(getFipsEnabledEnv()) ||
                Boolean.parseBoolean(getFipsEnabledLegacyEnv());
    }

    @VisibleForTesting
    protected String getFipsEnabledEnv()
    {
        return System.getenv(IS_FIPS_ENABLED);
    }

    @VisibleForTesting
    protected String getFipsEnabledLegacyEnv()
    {
        return System.getenv(IS_FIPS_ENABLED_LEGACY);
    }
}
