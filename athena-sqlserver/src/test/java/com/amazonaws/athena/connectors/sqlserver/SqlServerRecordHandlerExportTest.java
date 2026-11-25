/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcExportTypes;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlServerRecordHandlerExportTest
{
    private SqlServerRecordHandler handler;

    @Before
    public void setup()
    {
        System.setProperty("aws.region", "us-east-1");
        handler = new SqlServerRecordHandler(new DatabaseConnectionConfig("testCatalog", SqlServerConstants.NAME,
                "sqlserver://jdbc:sqlserver://hostname;databaseName=fakedatabase"), mock(S3Client.class),
                mock(SecretsManagerClient.class), mock(AthenaClient.class), mock(JdbcConnectionFactory.class),
                mock(JdbcSplitQueryBuilder.class), Map.of());
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
    public void testSqlServerColumnTypes() throws SQLException
    {
        Schema schema = handler.deriveExportSchema(result(
                new Object[] {"tiny", Types.TINYINT, 3, 0},
                new Object[] {"flag", Types.BIT, 1, 0},
                new Object[] {"money", Types.DECIMAL, 19, 4},
                new Object[] {"num", Types.NUMERIC, 18, 0},
                new Object[] {"ratio", Types.DOUBLE, 53, 0},
                new Object[] {"approx", Types.REAL, 24, 0},
                new Object[] {"created", Types.TIMESTAMP, 27, 7},
                new Object[] {"offset", microsoft.sql.Types.DATETIMEOFFSET, 34, 7},
                new Object[] {"at", Types.TIME, 16, 7},
                new Object[] {"id", Types.CHAR, 36, 0},
                new Object[] {"variant", microsoft.sql.Types.SQL_VARIANT, 8000, 0},
                new Object[] {"bytes", Types.VARBINARY, 8000, 0}));

        assertEquals(new ArrowType.Int(16, true), typeOf(schema, 0));
        assertEquals(ArrowType.Bool.INSTANCE, typeOf(schema, 1));
        assertEquals(JdbcExportTypes.decimal(19, 4), typeOf(schema, 2));
        assertEquals(JdbcExportTypes.decimal(18, 0), typeOf(schema, 3));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), typeOf(schema, 4));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), typeOf(schema, 5));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 6));
        assertEquals(JdbcExportTypes.TIMESTAMP_UTC, typeOf(schema, 7));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 8));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 9));
        assertEquals(ArrowType.Utf8.INSTANCE, typeOf(schema, 10));
        assertEquals(ArrowType.Binary.INSTANCE, typeOf(schema, 11));
    }

    @Test
    public void testDateTimeOffsetIsReadAsAnInstant() throws SQLException
    {
        ResultSet resultSet = result(new Object[] {"offset", microsoft.sql.Types.DATETIMEOFFSET, 34, 7});
        OffsetDateTime value = OffsetDateTime.of(2026, 10, 23, 5, 34, 56, 789_000_000, ZoneOffset.ofHours(-7));
        when(resultSet.getObject(1, OffsetDateTime.class)).thenReturn(value);

        Object read = handler.readExportValue(resultSet, 1, Field.nullable("offset", JdbcExportTypes.TIMESTAMP_UTC));

        assertEquals(value.toInstant(), ((ZonedDateTime) read).toInstant());
    }

    @Test
    public void testOtherColumnsUseTheMatchingGetter() throws SQLException
    {
        ResultSet resultSet = result(new Object[] {"id", Types.CHAR, 36, 0});
        when(resultSet.getString(1)).thenReturn("6F9619FF-8B86-D011-B42D-00C04FC964FF");

        assertEquals("6F9619FF-8B86-D011-B42D-00C04FC964FF",
                handler.readExportValue(resultSet, 1, Field.nullable("id", ArrowType.Utf8.INSTANCE)));
    }

    @Test
    public void testExportStatementFetchesInBatches() throws SQLException
    {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT 1")).thenReturn(statement);

        assertSame(statement, handler.prepareExportStatement(connection, "SELECT 1"));
        verify(statement).setFetchSize(1000);
    }
}
