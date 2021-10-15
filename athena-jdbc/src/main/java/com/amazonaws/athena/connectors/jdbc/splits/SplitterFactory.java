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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

/**
 * Creates splitter depending on split column data type.
 */
public class SplitterFactory
{
    /**
     * @param columnName split column name.
     * @param resultSet split min and max values.
     * @param maxSplits number of splits.
     * @return {@link Splitter} optional.
     * @throws SQLException exception accessing min and max values from {@link ResultSet}.
     */
    public Optional<Splitter> getSplitter(final String columnName, final ResultSet resultSet, final int maxSplits)
            throws SQLException
    {
        int type = resultSet.getMetaData().getColumnType(1);
        switch (type) {
            case Types.INTEGER:
                return Optional.of(new IntegerSplitter(new SplitInfo<>(new SplitRange<>(resultSet.getInt(1), resultSet.getInt(2)), columnName, type, maxSplits)));
            default:
               return Optional.empty();
        }
    }
}
