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
package com.amazonaws.connectors.athena.vertica;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;

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
        VerticaConnectionFactory verticaConnectionFactory = Mockito.mock(VerticaConnectionFactory.class);
        this.tableName = Mockito.mock(TableName.class);

        Mockito.when(verticaConnectionFactory.getOrCreateConn(anyString())).thenReturn(connection);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
    }

    @Test
    public void buildTableSchema() throws SQLException
    {
        String[] schema = {"TYPE_NAME", "COLUMN_NAME"};
        Object[][] values = {{"INTEGER", "testCol1"}};


        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);

        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(databaseMetaData.getColumns(null, tableName.getSchemaName(), tableName.getTableName(), null)).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);


        VerticaSchemaUtils verticaSchemaUtils = new VerticaSchemaUtils();
        Schema mockSchema = verticaSchemaUtils.buildTableSchema(this.connection, tableName);
        Field testField = mockSchema.findField("testCol1");
        org.junit.Assert.assertEquals(testField.getName(), "testCol1");

    }
}
