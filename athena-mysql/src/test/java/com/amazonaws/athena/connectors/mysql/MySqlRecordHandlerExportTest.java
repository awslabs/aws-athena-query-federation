/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcExportTypes;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MySqlRecordHandlerExportTest
{
    private MySqlRecordHandler handler;

    @Before
    public void setup()
    {
        handler = new MySqlRecordHandler(new DatabaseConnectionConfig("testCatalog", MySqlConstants.MYSQL_NAME,
                "mysql://jdbc:mysql://hostname/user=A&password=B"), mock(S3Client.class), mock(SecretsManagerClient.class),
                mock(AthenaClient.class), mock(JdbcConnectionFactory.class), mock(JdbcSplitQueryBuilder.class), Map.of());
    }

    /** Columns of a query result: label, JDBC type, precision, scale, signed. */
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
            when(metadata.isSigned(column)).thenReturn((Boolean) columns[i][4]);
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
    public void testUnsignedIntegersWidenToHoldTheirFullRange() throws SQLException
    {
        Schema schema = handler.deriveExportSchema(result(
                new Object[] {"u8", Types.TINYINT, 3, 0, false},
                new Object[] {"u16", Types.SMALLINT, 5, 0, false},
                new Object[] {"u32", Types.INTEGER, 10, 0, false},
                new Object[] {"u64", Types.BIGINT, 20, 0, false},
                new Object[] {"s32", Types.INTEGER, 10, 0, true}));

        assertEquals(new ArrowType.Int(16, true), typeOf(schema, 0));
        assertEquals(new ArrowType.Int(32, true), typeOf(schema, 1));
        assertEquals(new ArrowType.Int(64, true), typeOf(schema, 2));
        assertEquals(JdbcExportTypes.decimal(20, 0), typeOf(schema, 3));
        assertEquals(new ArrowType.Int(32, true), typeOf(schema, 4));
    }

    @Test
    public void testMySqlColumnTypes() throws SQLException
    {
        Schema schema = handler.deriveExportSchema(result(
                new Object[] {"flag", Types.BIT, 1, 0, true},
                new Object[] {"bits", Types.BIT, 8, 0, true},
                new Object[] {"amount", Types.DECIMAL, 12, 2, true},
                new Object[] {"born", Types.DATE, 10, 0, true},
                new Object[] {"updated", Types.TIMESTAMP, 26, 6, true},
                new Object[] {"at", Types.TIME, 8, 0, true},
                new Object[] {"doc", Types.LONGVARCHAR, 1073741824, 0, true},
                new Object[] {"payload", Types.LONGVARBINARY, 65535, 0, true}));

        assertEquals(ArrowType.Bool.INSTANCE, typeOf(schema, 0));
        assertEquals(ArrowType.Binary.INSTANCE, typeOf(schema, 1));
        assertEquals(JdbcExportTypes.decimal(12, 2), typeOf(schema, 2));
        assertEquals(new ArrowType.Date(DateUnit.DAY), typeOf(schema, 3));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 4));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 5));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 6));
        assertEquals(ArrowType.Binary.INSTANCE, typeOf(schema, 7));
    }

    @Test
    public void testDuplicateLabelsAreKept() throws SQLException
    {
        Schema schema = handler.deriveExportSchema(result(
                new Object[] {"id", Types.INTEGER, 10, 0, true},
                new Object[] {"id", Types.VARCHAR, 20, 0, true}));

        assertEquals(2, schema.getFields().size());
        assertEquals(new ArrowType.Int(32, true), typeOf(schema, 0));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 1));
    }

    @Test
    public void testTimeIsReadAsText() throws SQLException
    {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString(1)).thenReturn("838:59:59");

        assertEquals("838:59:59", handler.readExportValue(resultSet, 1, Field.nullable("at", ArrowType.Utf8.INSTANCE)));
    }

    @Test
    public void testExportStatementStreams() throws SQLException
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT 1")).thenReturn(statement);

        assertSame(statement, handler.prepareExportStatement(connection, "SELECT 1"));
        verify(statement).setFetchSize(Integer.MIN_VALUE);
    }
}
