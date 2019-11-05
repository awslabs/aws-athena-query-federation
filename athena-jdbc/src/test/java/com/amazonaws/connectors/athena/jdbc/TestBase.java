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
package com.amazonaws.connectors.athena.jdbc;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class TestBase
{
    protected ResultSet mockResultSet(String[] columnNames, int[] columnTypes, Object[][] rows, AtomicInteger rowNumber)
            throws SQLException
    {
        ResultSet resultSet = Mockito.mock(ResultSet.class, Mockito.RETURNS_DEEP_STUBS);

        Mockito.when(resultSet.next()).thenAnswer(
                (Answer<Boolean>) invocation -> {
                    if (rows.length <= 0 || rows[0].length <= 0) {
                        return false;
                    }

                    return rowNumber.getAndIncrement() + 1 < rows.length;
                });

        Mockito.when(resultSet.getInt(Mockito.any())).thenAnswer((Answer<Integer>) invocation -> {
            Object argument = invocation.getArguments()[0];

            if (argument instanceof Integer) {
                int colIndex = (Integer) argument;
                return (Integer) rows[rowNumber.get()][colIndex] - 1;
            }
            else if (argument instanceof String) {
                int colIndex = Arrays.asList(columnNames).indexOf(argument);
                return (Integer) rows[rowNumber.get()][colIndex];
            }
            else {
                throw new RuntimeException("Unexpected argument type " + argument.getClass());
            }
        });

        Mockito.when(resultSet.getString(Mockito.any())).thenAnswer((Answer<String>) invocation -> {
            Object argument = invocation.getArguments()[0];
            if (argument instanceof Integer) {
                int colIndex = (Integer) argument - 1;
                return String.valueOf(rows[rowNumber.get()][colIndex]);
            }
            else if (argument instanceof String) {
                int colIndex = Arrays.asList(columnNames).indexOf(argument);
                return String.valueOf(rows[rowNumber.get()][colIndex]);
            }
            else {
                throw new RuntimeException("Unexpected argument type " + argument.getClass());
            }
        });

        if (columnTypes != null) {
            Mockito.when(resultSet.getMetaData().getColumnCount()).thenReturn(columnNames.length);
            Mockito.when(resultSet.getMetaData().getColumnDisplaySize(Mockito.anyInt())).thenReturn(10);
            Mockito.when(resultSet.getMetaData().getColumnType(Mockito.anyInt())).thenAnswer((Answer<Integer>) invocation -> columnTypes[(Integer) invocation.getArguments()[0] - 1]);
        }

        return resultSet;
    }

    protected ResultSet mockResultSet(String[] columnNames, Object[][] rows, AtomicInteger rowNumber)
            throws SQLException
    {
        return this.mockResultSet(columnNames, null, rows, rowNumber);
    }
}
