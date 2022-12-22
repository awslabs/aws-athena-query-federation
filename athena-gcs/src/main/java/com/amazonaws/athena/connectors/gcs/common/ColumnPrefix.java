/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.common;

public class ColumnPrefix
{
    // Partition column name
    private String column;
    // Regular expression prefix for the column
    // So if the regular expression for a folder is (year=)(\d+) where (year=) is a capture group to capture
    // of a column prefix, this property will hold 'year='
    private String prefix;

    public ColumnPrefix(String column, String prefix)
    {
        this.column = column;
        this.prefix = prefix;
    }

    public String getColumn()
    {
        return column;
    }

    public ColumnPrefix column(String column)
    {
        this.column = column;
        return this;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public ColumnPrefix prefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }
}
