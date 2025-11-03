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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.sql.Types;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VerticaSchemaUtilsTest extends TestBase
{
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private TableName tableName;

    @Mock
    private X509Certificate mockCertificate;

    @Before
    public void setUp() throws SQLException
    {
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        this.databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        this.tableName = Mockito.mock(TableName.class);

        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
    }

    @Test
    public void buildTableSchema() throws SQLException
    {
     /*   String[] schema = {"TYPE_NAME", "COLUMN_NAME"};
        Object[][] values = {{"INTEGER", "testCol1"}, {"INTEGER", "testCol2"}};
        int [] types = {Types.INTEGER, Types.INTEGER};*/

        String[] schema = {"TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME"};
        Object[][] values = {
                {"testSchema", "testTable1", "bit_col", "BIT"},
                {"testSchema", "testTable1", "tinyint_col", "TINYINT"},
                {"testSchema", "testTable1", "smallint_col", "SMALLINT"},
                {"testSchema", "testTable1", "integer_col", "INTEGER"},
                {"testSchema", "testTable1", "bigint_col", "BIGINT"},
                {"testSchema", "testTable1", "float4_col", "FLOAT4"},
                {"testSchema", "testTable1", "float8_col", "FLOAT8"},
                {"testSchema", "testTable1", "numeric_col", "NUMERIC"},
                {"testSchema", "testTable1", "boolean_col", "BOOLEAN"},
                {"testSchema", "testTable1", "varchar_col", "VARCHAR"},
                {"testSchema", "testTable1", "timestamp_col", "TIMESTAMP"},
                {"testSchema", "testTable1", "timestamptz_col", "TIMESTAMPTZ"},
                {"testSchema", "testTable1", "datetime_col", "DATETIME"},
                {"testSchema", "testTable1", "unknown_col", "UNKNOWN"}
        };
        int[] types = {
                Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.FLOAT, Types.DOUBLE, Types.NUMERIC, Types.BOOLEAN, Types.VARCHAR,
                Types.TIMESTAMP, Types.TIMESTAMP, Types.DATE, Types.OTHER
        };

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(databaseMetaData.getColumns(null, tableName.getSchemaName(), tableName.getTableName(), null)).thenReturn(resultSet);

        VerticaSchemaUtils verticaSchemaUtils = new VerticaSchemaUtils();
        Schema mockSchema = verticaSchemaUtils.buildTableSchema(this.connection, tableName);

        assertEquals("Bool", mockSchema.findField("bit_col").getType().toString());
        assertEquals("Int(8, true)", mockSchema.findField("tinyint_col").getType().toString());
        assertEquals("Int(16, true)", mockSchema.findField("smallint_col").getType().toString());
        assertEquals("Int(64, true)", mockSchema.findField("integer_col").getType().toString());
        assertEquals("Int(64, true)", mockSchema.findField("bigint_col").getType().toString());
        assertEquals("FloatingPoint(SINGLE)", mockSchema.findField("float4_col").getType().toString());
        assertEquals("FloatingPoint(DOUBLE)", mockSchema.findField("float8_col").getType().toString());
        assertEquals("Decimal(10, 2, 128)", mockSchema.findField("numeric_col").getType().toString());
        assertEquals("Utf8", mockSchema.findField("boolean_col").getType().toString());
        assertEquals("Utf8", mockSchema.findField("varchar_col").getType().toString());
        assertEquals("Utf8", mockSchema.findField("timestamp_col").getType().toString());
        assertEquals("Utf8", mockSchema.findField("timestamptz_col").getType().toString());
        assertEquals("Date(DAY)", mockSchema.findField("datetime_col").getType().toString());
        assertEquals("Utf8", mockSchema.findField("unknown_col").getType().toString());
    }

    @Test
    public void buildTableSchemaSQLException() throws SQLException
    {
        when(databaseMetaData.getColumns(null, tableName.getSchemaName(), tableName.getTableName(), null))
                .thenThrow(new SQLException("Database error"));

        VerticaSchemaUtils verticaSchemaUtils = new VerticaSchemaUtils();
        try {
            verticaSchemaUtils.buildTableSchema(connection, tableName);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Error in building the table schema"));
            assertTrue(e.getCause() instanceof SQLException);
        }
    }

    @Test
    public void formatCrtFileContents() throws Exception
    {
        when(mockCertificate.getEncoded()).thenReturn("test-cert".getBytes());

        Method formatMethod = VerticaSchemaUtils.class.getDeclaredMethod("formatCrtFileContents", Certificate.class);
        formatMethod.setAccessible(true);
        String result = (String) formatMethod.invoke(null, mockCertificate);

        String expectedEncoded = Base64.getMimeEncoder(64, System.lineSeparator().getBytes())
                .encodeToString("test-cert".getBytes());
        String expected = "-----BEGIN CERTIFICATE-----" + System.lineSeparator() +
                expectedEncoded + System.lineSeparator() + "-----END CERTIFICATE-----";
        assertEquals(expected, result);
    }

    @Test
    public void formatCrtFileContents_NullCertificate() throws Exception
    {
        Method formatMethod = VerticaSchemaUtils.class.getDeclaredMethod("formatCrtFileContents", Certificate.class);
        formatMethod.setAccessible(true);

        try {
            formatMethod.invoke(null, (Certificate) null);
            fail("Expected NullPointerException for null certificate");
        } catch (java.lang.reflect.InvocationTargetException e) {
            assertTrue("Should throw NullPointerException for null certificate",
                e.getCause() instanceof NullPointerException);
        }
    }

}