/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.splits;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

public class SplitterFactoryTest
{
    private static final String TEST_COLUMN_NAME = "testColumn";
    private static final int MAX_SPLITS = 10;
    private ResultSet resultSet;
    private SplitterFactory splitterFactory;

    @Before
    public void setup() {
        resultSet = Mockito.mock(ResultSet.class, Mockito.RETURNS_DEEP_STUBS);
        splitterFactory = new SplitterFactory();
    }

    @Test
    public void getIntegerSplitter()
            throws SQLException
    {
        Mockito.when(resultSet.getMetaData().getColumnType(1)).thenReturn(Types.INTEGER);
        Optional<Splitter> splitter = splitterFactory.getSplitter(TEST_COLUMN_NAME, resultSet, MAX_SPLITS);
        Assert.assertTrue(splitter.isPresent());
        Assert.assertEquals(IntegerSplitter.class, splitter.get().getClass());
    }

    @Test
    public void getStringSplitter()
            throws SQLException
    {
        Mockito.when(resultSet.getMetaData().getColumnType(1)).thenReturn(Types.VARCHAR);
        Assert.assertFalse(splitterFactory.getSplitter(TEST_COLUMN_NAME, resultSet, MAX_SPLITS).isPresent());
    }

    @Test
    public void getFloatSplitter()
            throws SQLException
    {
        Mockito.when(resultSet.getMetaData().getColumnType(1)).thenReturn(Types.FLOAT);
        Assert.assertFalse(splitterFactory.getSplitter(TEST_COLUMN_NAME, resultSet, MAX_SPLITS).isPresent());
    }

    @Test
    public void getDoubleSplitter()
            throws SQLException
    {
        Mockito.when(resultSet.getMetaData().getColumnType(1)).thenReturn(Types.DOUBLE);
        Assert.assertFalse(splitterFactory.getSplitter(TEST_COLUMN_NAME, resultSet, MAX_SPLITS).isPresent());
    }

    @Test
    public void getDateSplitter()
            throws SQLException
    {
        Mockito.when(resultSet.getMetaData().getColumnType(1)).thenReturn(Types.DATE);
        Assert.assertFalse(splitterFactory.getSplitter(TEST_COLUMN_NAME, resultSet, MAX_SPLITS).isPresent());
    }

    @Test
    public void getDecimalSplitter()
            throws SQLException
    {
        Mockito.when(resultSet.getMetaData().getColumnType(1)).thenReturn(Types.DECIMAL);
        Assert.assertFalse(splitterFactory.getSplitter(TEST_COLUMN_NAME, resultSet, MAX_SPLITS).isPresent());
    }
}
