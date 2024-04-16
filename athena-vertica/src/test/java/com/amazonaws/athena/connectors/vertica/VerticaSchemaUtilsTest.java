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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.nullable;

public class VerticaSchemaUtilsTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaSchemaUtils.class);
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private TableName tableName;


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
        Object[][] values = {{"testSchema", "testTable1", "id", "bigint"}, {"testSchema", "testTable1", "date", "timestamp"},
                {"testSchema", "testTable1", "orders", "integer"}, {"testSchema", "testTable1", "price", "float4"},
                {"testSchema", "testTable1", "shop", "varchar"}
               };
        int[] types = {Types.BIGINT, Types.TIMESTAMP, Types.INTEGER,Types.FLOAT, Types.VARCHAR, Types.VARCHAR};

        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, types, values, rowNumber);

        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(databaseMetaData.getColumns(null, tableName.getSchemaName(), tableName.getTableName(), null)).thenReturn(resultSet);

        VerticaSchemaUtils verticaSchemaUtils = new VerticaSchemaUtils();
        Schema mockSchema = verticaSchemaUtils.buildTableSchema(this.connection, tableName);

        Field testDateField = mockSchema.findField("date");
        Assert.assertEquals("Utf8", testDateField.getType().toString());

        Field testPriceField = mockSchema.findField("price");
        Assert.assertEquals("FloatingPoint(SINGLE)", testPriceField.getType().toString());


    }
}
