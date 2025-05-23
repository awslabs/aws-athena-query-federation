/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.sun.jna.platform.unix.LibC;
import org.apache.arrow.vector.types.pojo.Schema;
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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;

import static com.amazonaws.athena.connectors.vertica.VerticaConstants.SSL_CERT_FILE_LOCATION;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.SSL_CERT_FILE_LOCATION_VALUE;

public class VerticaSchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaSchemaUtils.class);

    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    //Builds the table schema
    protected Schema buildTableSchema(Connection connection, TableName name)
    {
        try
        {
            logger.info("Building the schema for table {} ", name);
            SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();

            DatabaseMetaData dbMetadata = connection.getMetaData();
            ResultSet definition = dbMetadata.getColumns(null, name.getSchemaName(), name.getTableName(), null);
            while(definition.next())
            {
                String colType = definition.getString("TYPE_NAME").toUpperCase();
                convertToArrowType(tableSchemaBuilder, definition.getString("COLUMN_NAME"), colType);
            }
            return tableSchemaBuilder.build();

        }
        catch(SQLException e)
        {
            throw new RuntimeException("Error in building the table schema: " + e.getMessage(), e);
        }

    }

    public static void convertToArrowType(SchemaBuilder tableSchemaBuilder, String colName, String colType) throws SQLException
    {
        switch (colType)
        {
            //If Bit
            case "BIT":
            {
                tableSchemaBuilder.addBitField(colName);
                break;
            }
            //If TinyInt
            case "TINYINT":
            {
                tableSchemaBuilder.addTinyIntField(colName);
                break;
            }
            //If SmallInt
            case "SMALLINT":
            {
                tableSchemaBuilder.addSmallIntField(colName);
                break;
            }
            //If Int
            case "INTEGER":
                //If BIGINT
            case "BIGINT": {
                tableSchemaBuilder.addBigIntField(colName);
                break;
            }
            //If FLOAT4
            case "FLOAT4":
            {
                tableSchemaBuilder.addFloat4Field(colName);
                break;
            }
            //If FLOAT8
            case "FLOAT8":
            {
                tableSchemaBuilder.addFloat8Field(colName);
                break;
            }
            //If DECIMAL/NUMERIC
            case "NUMERIC":
            {
                tableSchemaBuilder.addDecimalField(colName, 10, 2);
                break;
            }
            //If VARCHAR
            case "BOOLEAN":
            case "VARCHAR":
            case "TIMESTAMPTZ":
            case "TIMESTAMP": {
                tableSchemaBuilder.addStringField(colName);
                break;
            }
            //If DATETIME
            case "DATETIME":
            {
                tableSchemaBuilder.addDateDayField(colName);
                break;
            }

            default:
                tableSchemaBuilder.addStringField(colName);
        }
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
