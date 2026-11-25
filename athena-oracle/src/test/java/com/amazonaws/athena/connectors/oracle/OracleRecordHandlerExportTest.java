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

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcExportTypes;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import oracle.jdbc.OracleTypes;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OracleRecordHandlerExportTest
{
    private OracleRecordHandler handler;

    @Before
    public void setup()
    {
        handler = new OracleRecordHandler(new DatabaseConnectionConfig("testCatalog", OracleConstants.ORACLE_NAME,
                "oracle://jdbc:oracle:thin:username/password@//127.0.0.1:1521/orcl"), mock(S3Client.class),
                mock(SecretsManagerClient.class), mock(AthenaClient.class), mock(JdbcConnectionFactory.class),
                mock(JdbcSplitQueryBuilder.class), Map.of("default_scale", "2"));
    }

    /** Columns of a query result: label, JDBC type, precision, scale. */
    private static ResultSet result(Object[]... columns) throws SQLException
    {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(columns.length);
        for (int i = 0; i < columns.length; i++) {
            int column = i + 1;
            when(metadata.getColumnLabel(column)).thenReturn((String) columns[i][0]);
            when(metadata.getColumnName(column)).thenReturn((String) columns[i][0]);
            when(metadata.getColumnType(column)).thenReturn((Integer) columns[i][1]);
            when(metadata.getPrecision(column)).thenReturn((Integer) columns[i][2]);
            when(metadata.getScale(column)).thenReturn((Integer) columns[i][3]);
            when(metadata.isSigned(column)).thenReturn(true);
        }
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(metadata);
        return resultSet;
    }

    private static ArrowType typeOf(Schema schema, int index)
    {
        return schema.getFields().get(index).getType();
    }

    @Test
    public void testOracleNumberTypes() throws SQLException
    {
        Schema schema = handler.deriveExportSchema(result(
                new Object[] {"ID", Types.NUMERIC, 10, 0},
                new Object[] {"AMOUNT", Types.NUMERIC, 12, 2},
                new Object[] {"ANY_NUMBER", Types.NUMERIC, 0, -127},
                new Object[] {"FLOAT_COL", Types.NUMERIC, 126, -127},
                new Object[] {"ROUNDED", Types.NUMERIC, 3, -2},
                new Object[] {"BF", OracleTypes.BINARY_FLOAT, 0, 0},
                new Object[] {"BD", OracleTypes.BINARY_DOUBLE, 0, 0}));

        assertEquals(JdbcExportTypes.decimal(10, 0), typeOf(schema, 0));
        assertEquals(JdbcExportTypes.decimal(12, 2), typeOf(schema, 1));
        assertEquals(JdbcExportTypes.decimal(38, 2), typeOf(schema, 2));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), typeOf(schema, 3));
        assertEquals(JdbcExportTypes.decimal(5, 0), typeOf(schema, 4));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), typeOf(schema, 5));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), typeOf(schema, 6));
    }

    @Test
    public void testOracleTemporalAndLobTypes() throws SQLException
    {
        Schema schema = handler.deriveExportSchema(result(
                new Object[] {"DATE_COL", Types.TIMESTAMP, 7, 0},
                new Object[] {"TS", Types.TIMESTAMP, 11, 6},
                new Object[] {"TSTZ", OracleTypes.TIMESTAMPTZ, 13, 6},
                new Object[] {"TSLTZ", OracleTypes.TIMESTAMPLTZ, 11, 6},
                new Object[] {"IYM", OracleTypes.INTERVALYM, 0, 0},
                new Object[] {"IDS", OracleTypes.INTERVALDS, 0, 0},
                new Object[] {"NOTES", Types.CLOB, 0, 0},
                new Object[] {"NNOTES", Types.NCLOB, 0, 0},
                new Object[] {"DOC", Types.BLOB, 0, 0},
                new Object[] {"RID", Types.ROWID, 0, 0},
                new Object[] {"NAME", Types.VARCHAR, 100, 0}));

        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 0));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 1));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 2));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 3));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 4));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 5));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 6));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 7));
        assertEquals(ArrowType.Binary.INSTANCE, typeOf(schema, 8));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 9));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 10));
    }

    @Test
    public void testTimestampWithTimeZoneIsReadAsAnInstant() throws SQLException
    {
        ResultSet resultSet = result(new Object[] {"TSTZ", OracleTypes.TIMESTAMPTZ, 13, 6});
        OffsetDateTime value = OffsetDateTime.of(2026, 10, 23, 5, 34, 56, 789_000_000, ZoneOffset.ofHours(-7));
        when(resultSet.getObject(1, OffsetDateTime.class)).thenReturn(value);

        Object read = handler.readExportValue(resultSet, 1, Field.nullable("TSTZ", JdbcExportTypes.TIMESTAMP_UTC));

        assertEquals(value.toInstant(), ((ZonedDateTime) read).toInstant());
    }

    @Test
    public void testLobsAreReadAsTextAndBytes() throws SQLException
    {
        ResultSet resultSet = result(new Object[] {"NOTES", Types.CLOB, 0, 0}, new Object[] {"DOC", Types.BLOB, 0, 0});
        when(resultSet.getString(1)).thenReturn("long text");
        Blob blob = mock(Blob.class);
        when(blob.length()).thenReturn(2L);
        when(blob.getBytes(1, 2)).thenReturn(new byte[] {7, 8});
        when(resultSet.getObject(2)).thenReturn(blob);

        assertEquals("long text", handler.readExportValue(resultSet, 1, Field.nullable("NOTES", ArrowType.Utf8.INSTANCE)));
        assertArrayEquals(new byte[] {7, 8},
                (byte[]) handler.readExportValue(resultSet, 2, Field.nullable("DOC", ArrowType.Binary.INSTANCE)));
    }

    @Test
    public void testExportStatementFetchesInBatches() throws SQLException
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT 1 FROM DUAL")).thenReturn(statement);

        assertSame(statement, handler.prepareExportStatement(connection, "SELECT 1 FROM DUAL"));
        verify(statement).setFetchSize(1000);
    }
}
