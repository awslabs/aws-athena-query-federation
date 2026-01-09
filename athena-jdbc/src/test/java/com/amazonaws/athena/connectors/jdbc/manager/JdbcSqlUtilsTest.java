/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;
import org.stringtemplate.v4.ST;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcSqlUtilsTest
{
    @Test
    public void quoteIdentifier_StandardIdentifier_ReturnsQuotedIdentifier()
    {
        // Test with double quotes
        Assert.assertEquals("\"test\"", JdbcSqlUtils.quoteIdentifier("test", "\""));
        
        // Test with backticks
        Assert.assertEquals("`test`", JdbcSqlUtils.quoteIdentifier("test", "`"));
        
        // Test with escaped quotes
        Assert.assertEquals("\"test\"\"quote\"", JdbcSqlUtils.quoteIdentifier("test\"quote", "\""));
        
        // Test empty string
        Assert.assertEquals("\"\"", JdbcSqlUtils.quoteIdentifier("", "\""));
        
        // Test with multiple quotes
        Assert.assertEquals("\"test\"\"\"\"quote\"", JdbcSqlUtils.quoteIdentifier("test\"\"quote", "\""));
        
        // Test with special characters
        Assert.assertEquals("\"test_column\"", JdbcSqlUtils.quoteIdentifier("test_column", "\""));
        Assert.assertEquals("\"test-column\"", JdbcSqlUtils.quoteIdentifier("test-column", "\""));
        Assert.assertEquals("\"test.column\"", JdbcSqlUtils.quoteIdentifier("test.column", "\""));
        
        // Test with single quote character
        Assert.assertEquals("'test'", JdbcSqlUtils.quoteIdentifier("test", "'"));
        Assert.assertEquals("'test''quote'", JdbcSqlUtils.quoteIdentifier("test'quote", "'"));
    }

    @Test
    public void renderTemplate_ValidTemplateAndParams_ReturnsRenderedString()
    {
        JdbcQueryFactory mockFactory = mock(JdbcQueryFactory.class);
        ST mockST = new ST("SELECT <columnName> FROM <table>");
        when(mockFactory.getQueryTemplate(anyString())).thenReturn(mockST);

        Map<String, Object> params = new HashMap<>();
        params.put("columnName", "col1");
        params.put("table", "tbl1");

        String result = JdbcSqlUtils.renderTemplate(mockFactory, "select", params);
        Assert.assertNotNull("Result should not be null", result);
        Assert.assertEquals("SELECT col1 FROM tbl1", result);
    }
    
    @Test(expected = RuntimeException.class)
    public void renderTemplate_TemplateNotFound_ThrowsRuntimeException()
    {
        JdbcQueryFactory mockFactory = mock(JdbcQueryFactory.class);
        when(mockFactory.getQueryTemplate(anyString())).thenReturn(null);
        
        JdbcSqlUtils.renderTemplate(mockFactory, "notfound", new HashMap<>());
    }
    
    
    @Test
    public void setParameters_VariousArrowTypes_SetsCorrectStatementParameters() throws SQLException
    {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        List<TypeAndValue> parameterValues = new ArrayList<>();

        parameterValues.add(new TypeAndValue(Types.MinorType.BIGINT.getType(), 100L));
        parameterValues.add(new TypeAndValue(Types.MinorType.INT.getType(), 10));
        parameterValues.add(new TypeAndValue(Types.MinorType.SMALLINT.getType(), (short) 1));
        parameterValues.add(new TypeAndValue(Types.MinorType.TINYINT.getType(), (byte) 2));
        parameterValues.add(new TypeAndValue(Types.MinorType.FLOAT8.getType(), 1.23d));
        parameterValues.add(new TypeAndValue(Types.MinorType.FLOAT4.getType(), 4.56f));
        parameterValues.add(new TypeAndValue(Types.MinorType.BIT.getType(), true));
        parameterValues.add(new TypeAndValue(Types.MinorType.VARCHAR.getType(), "test"));
        parameterValues.add(new TypeAndValue(Types.MinorType.VARBINARY.getType(), new byte[]{1, 2}));
        parameterValues.add(new TypeAndValue(new ArrowType.Decimal(10, 2, 128), new BigDecimal("123.45")));

        // Date testing
        long epochDays = 18262; // 2020-01-01
        parameterValues.add(new TypeAndValue(Types.MinorType.DATEDAY.getType(), epochDays));

        // Timestamp testing
        LocalDateTime now = LocalDateTime.now();
        parameterValues.add(new TypeAndValue(Types.MinorType.DATEMILLI.getType(), now));

        JdbcSqlUtils.setParameters(mockStatement, parameterValues);

        verify(mockStatement).setLong(1, 100L);
        verify(mockStatement).setInt(2, 10);
        verify(mockStatement).setShort(3, (short) 1);
        verify(mockStatement).setByte(4, (byte) 2);
        verify(mockStatement).setDouble(5, 1.23d);
        verify(mockStatement).setFloat(6, 4.56f);
        verify(mockStatement).setBoolean(7, true);
        verify(mockStatement).setString(8, "test");
        verify(mockStatement).setBytes(9, new byte[]{1, 2});
        verify(mockStatement).setBigDecimal(10, new BigDecimal("123.45"));

        // Verify Date
        long utcMillis = TimeUnit.DAYS.toMillis(epochDays);
        int offset = TimeZone.getDefault().getOffset(utcMillis);
        verify(mockStatement).setDate(11, new Date(utcMillis - offset));

        // Verify Timestamp
        verify(mockStatement).setTimestamp(12, new Timestamp(now.toInstant(ZoneOffset.UTC).toEpochMilli()));
    }
    
    @Test
    public void setParameters_EmptyList_NoParametersSet() throws SQLException
    {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        List<TypeAndValue> emptyList = new ArrayList<>();
        
        JdbcSqlUtils.setParameters(mockStatement, emptyList);
        
        // Verify no parameters were set
        verify(mockStatement, never()).setLong(anyInt(), anyLong());
        verify(mockStatement, never()).setInt(anyInt(), anyInt());
        verify(mockStatement, never()).setString(anyInt(), anyString());
    }
    
    @Test(expected = AthenaConnectorException.class)
    public void setParameters_UnsupportedType_ThrowsAthenaConnectorException() throws SQLException
    {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        List<TypeAndValue> parameterValues = new ArrayList<>();
        ArrowType.List listType = new ArrowType.List();
        parameterValues.add(new TypeAndValue(listType, Collections.emptyList()));
        JdbcSqlUtils.setParameters(mockStatement, parameterValues);
        Assert.fail("Expected AthenaConnectorException to be thrown for unsupported type");
        
    }
    
    @Test
    public void setParameters_MultipleParametersOfSameType_SetsAllCorrectly() throws SQLException
    {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        List<TypeAndValue> parameterValues = new ArrayList<>();
        
        parameterValues.add(new TypeAndValue(Types.MinorType.INT.getType(), 10));
        parameterValues.add(new TypeAndValue(Types.MinorType.INT.getType(), 20));
        parameterValues.add(new TypeAndValue(Types.MinorType.INT.getType(), 30));
        
        JdbcSqlUtils.setParameters(mockStatement, parameterValues);
        
        verify(mockStatement).setInt(1, 10);
        verify(mockStatement).setInt(2, 20);
        verify(mockStatement).setInt(3, 30);
    }
    
    @Test
    public void setParameters_DecimalPrecision_HandlesCorrectly() throws SQLException
    {
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        List<TypeAndValue> parameterValues = new ArrayList<>();
        
        parameterValues.add(new TypeAndValue(new ArrowType.Decimal(10, 2, 128), new BigDecimal("123.45")));
        parameterValues.add(new TypeAndValue(new ArrowType.Decimal(20, 5, 128), new BigDecimal("99999.99999")));
        parameterValues.add(new TypeAndValue(new ArrowType.Decimal(5, 0, 128), new BigDecimal("12345")));
        
        JdbcSqlUtils.setParameters(mockStatement, parameterValues);
        
        verify(mockStatement).setBigDecimal(1, new BigDecimal("123.45"));
        verify(mockStatement).setBigDecimal(2, new BigDecimal("99999.99999"));
        verify(mockStatement).setBigDecimal(3, new BigDecimal("12345"));
    }
}
