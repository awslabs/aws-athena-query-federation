/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtils
{
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {}

    public static SchemaBuilder makeSchema()
    {
        return SchemaBuilder.newBuilder()
                .addStringField("family1:col1")
                .addStringField("family1:col2")
                .addBigIntField("family1:col3")
                .addStringField("family1:col10")
                .addFloat8Field("family1:col20")
                .addBigIntField("family1:col30")
                .addStringField("family2:col1")
                .addFloat8Field("family2:col2")
                .addBigIntField("family2:col3")
                .addStringField("family2:col10")
                .addFloat8Field("family2:col20")
                .addBigIntField("family2:col30")
                .addBigIntField("family3:col1");
    }

    public static List<Result> makeResults()
    {
        List<Result> results = new ArrayList<>();
        //Initial row with two column familyies and a mix of columns and types
        results.add(makeResult("family1", "col1", "varchar",
                "family1", "col2", "1.0",
                "family1", "col3", "1",
                "family2", "col1", "varchar",
                "family2", "col2", "1.0",
                "family2", "col3", "1"
        ));

        //Add a row which has only new columns to ensure we get a union
        results.add(makeResult("family1", "col10", "varchar",
                "family1", "col20", "1.0",
                "family1", "col30", "1",
                "family2", "col10", "varchar",
                "family2", "col20", "1.0",
                "family2", "col30", "1",
                "family3", "col1", "1"
        ));

        //Add a 2nd occurance of col2 in family1 but with a conflicting type, it should result in a final type of varchar
        results.add(makeResult("family1", "col2", "1"));

        return results;
    }

    public static List<Result> makeResults(int numResults)
    {
        List<Result> results = new ArrayList<>();

        for (int i = 0; i < numResults; i++) {
            //Initial row with two column familyies and a mix of columns and types
            results.add(makeResult("family1", "col1", "varchar" + i,
                    "family1", "col2", String.valueOf(i * 1.1),
                    "family1", "col3", String.valueOf(i),
                    "family2", "col1", "varchar" + i,
                    "family2", "col2", String.valueOf(i * 1.1),
                    "family2", "col3", String.valueOf(i)
            ));

            //Add a row which has only new columns to ensure we get a union
            results.add(makeResult("family1", "col10", "varchar" + i,
                    "family1", "col20", String.valueOf(i * 1.1),
                    "family1", "col30", String.valueOf(i),
                    "family2", "col10", "varchar" + i,
                    "family2", "col20", String.valueOf(i * 1.1),
                    "family2", "col30", String.valueOf(i),
                    "family3", "col1", String.valueOf(i)
            ));

            //Add a 2nd occurance of col2 in family1 but with a conflicting type, it should result in a final type of varchar
            results.add(makeResult("family1", "col2", String.valueOf(i)));
        }

        return results;
    }

    public static Result makeResult(String... values)
    {
        if (values.length % 3 != 0) {
            throw new RuntimeException("Method requires values in multiples of 3 -> family, qualifier, value");
        }

        List<Cell> result = new ArrayList<>();
        Map<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < values.length; i += 3) {
            Cell mockCell = mock(Cell.class);
            Mockito.lenient().when(mockCell.getFamilyArray()).thenReturn(values[i].getBytes());
            Mockito.lenient().when(mockCell.getFamilyOffset()).thenReturn(0);
            Mockito.lenient().when(mockCell.getFamilyLength()).thenReturn((byte)values[i].getBytes().length);

            Mockito.lenient().when(mockCell.getQualifierArray()).thenReturn(values[i + 1].getBytes());
            Mockito.lenient().when(mockCell.getQualifierOffset()).thenReturn(0);
            Mockito.lenient().when(mockCell.getQualifierLength()).thenReturn(values[i + 1].getBytes().length);

            Mockito.lenient().when(mockCell.getValueArray()).thenReturn(values[i + 2].getBytes());
            Mockito.lenient().when(mockCell.getValueOffset()).thenReturn(0);
            Mockito.lenient().when(mockCell.getValueLength()).thenReturn(values[i + 2].getBytes().length);

            valueMap.put(values[i] + ":" + values[i + 1], values[i + 2]);
            result.add(mockCell);
        }

        Result mockResult = mock(Result.class);
        Mockito.lenient().when(mockResult.listCells()).thenReturn(result);
        Mockito.lenient().when(mockResult.getValue(nullable(byte[].class), nullable(byte[].class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    String family = new String(invocation.getArgument(0, byte[].class));
                    String column = new String(invocation.getArgument(1, byte[].class));
                    String key = family + ":" + column;
                    if (!valueMap.containsKey(key)) {
                        return null;
                    }
                    else {
                        return valueMap.get(key).getBytes();
                    }
                });
        return mockResult;
    }

    public static HRegionInfo makeRegion(int id, String schema, String table)
    {
        return new HRegionInfo(id, org.apache.hadoop.hbase.TableName.valueOf(schema, table), 1);
    }
}
