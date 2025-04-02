package com.amazonaws.athena.connectors.snowflake;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class SnowflakeCaseInsensitiveResolverTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ResultSet mockResultSet;

    private Map<String, String> configOptions;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        configOptions = new HashMap<>();

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    }
    //@Test
    void testCaseInsensitiveSearchMode() throws SQLException {
        configOptions.put("casing_mode", "CASE_INSENSITIVE_SEARCH");
        TableName inputTable = new TableName("TestSchema", "TestTable");

        // Mock case-insensitive resolution
        SnowflakeCaseInsensitiveResolver spyResolver = Mockito.spy(SnowflakeCaseInsensitiveResolver.class);
        when(spyResolver.getSchemaNameCaseInsensitively(any(), any(), any())).thenReturn("testschema");
        when(spyResolver.getTableNameCaseInsensitively(any(), any(), any(), any())).thenReturn("testtable");

        TableName result = SnowflakeCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTable, configOptions);

        assertEquals("testschema", result.getSchemaName());
        assertEquals("testtable", result.getTableName());
    }

    @Test
    void testAnnotationMode() throws SQLException
    {
        configOptions.put("casing_mode", "ANNOTATION");
        TableName inputTable = new TableName("TestSchema", "TestTable@schemaCase=lower&tableCase=upper");

        TableName result = SnowflakeCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTable, configOptions);

        assertEquals("testschema", result.getSchemaName());
        assertEquals("TESTTABLE", result.getTableName());
    }

   // @Test
    void testNoneMode() throws SQLException
    {
        configOptions.put("casing_mode", "NONE");
        TableName inputTable = new TableName("TestSchema", "TestTable");

        TableName result = SnowflakeCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTable, configOptions);

        assertEquals("TestSchema", result.getSchemaName());
        assertEquals("TestTable", result.getTableName());
    }

    @Test
    void testInvalidCasingModeDefaultsToNone() {
        configOptions.put("casing_mode", "INVALID_MODE");
        TableName inputTable = new TableName("TestSchema", "TestTable");

        assertThrows(Exception.class, () -> {
            SnowflakeCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTable, configOptions);
        });
    }

    @Test
    void testGetSchemaNameCaseInsensitively_Success() throws SQLException {
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getString("SCHEMA_NAME")).thenReturn("TestSchema");

        String result = SnowflakeCaseInsensitiveResolver.getSchemaNameCaseInsensitively(mockConnection, "testschema", configOptions);
        assertEquals("TestSchema", result);
    }

    @Test
    void testGetSchemaNameCaseInsensitively_NoMatch() throws SQLException {
        when(mockResultSet.next()).thenReturn(false);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            SnowflakeCaseInsensitiveResolver.getSchemaNameCaseInsensitively(mockConnection, "unknownschema", configOptions);
        });

        assertTrue(exception.getMessage().contains("Schema name case insensitive match failed"));
    }

    @Test
    void testGetTableNameCaseInsensitively_Success() throws SQLException {
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getString("TABLE_NAME")).thenReturn("TestTable");

        String result = SnowflakeCaseInsensitiveResolver.getTableNameCaseInsensitively(mockConnection, "TestSchema", "testtable", configOptions);
        assertEquals("TestTable", result);
    }

    @Test
    void testGetTableNameCaseInsensitively_NoMatch() throws SQLException {
        when(mockResultSet.next()).thenReturn(false);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            SnowflakeCaseInsensitiveResolver.getTableNameCaseInsensitively(mockConnection, "TestSchema", "unknownTable", configOptions);
        });

        assertTrue(exception.getMessage().contains("Schema name case insensitive match failed"));
    }
}