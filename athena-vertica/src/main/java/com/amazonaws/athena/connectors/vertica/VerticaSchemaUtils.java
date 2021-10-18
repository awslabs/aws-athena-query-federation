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

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class VerticaSchemaUtils
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaSchemaUtils.class);

    //Builds the table schema
    protected Schema buildTableSchema(Connection connection, TableName name)
    {
        try
        {
            logger.info("Building the schema for table {} ", name);
            SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();

            DatabaseMetaData dbMetadata = connection.getMetaData();
            ResultSet definition = dbMetadata.getColumns(null, name.getSchemaName(), name.getTableName(), null);
            while(definition.next())
            {
                String colType = definition.getString("TYPE_NAME").toUpperCase();
                switch (colType)
                {
                    //If Bit
                    case "BIT":
                    {
                        tableSchemaBuilder.addBitField(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If TinyInt
                    case "TINYINT":
                    {
                        tableSchemaBuilder.addTinyIntField(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If SmallInt
                    case "SMALLINT":
                    {
                        tableSchemaBuilder.addSmallIntField(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If Int
                    case "INTEGER":
                        //If BIGINT
                    case "BIGINT": {
                        tableSchemaBuilder.addBigIntField(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If FLOAT4
                    case "FLOAT4":
                    {
                        tableSchemaBuilder.addFloat4Field(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If FLOAT8
                    case "FLOAT8":
                    {
                        tableSchemaBuilder.addFloat8Field(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If DECIMAL/NUMERIC
                    case "NUMERIC":
                    {
                        tableSchemaBuilder.addDecimalField(definition.getString("COLUMN_NAME"), 10, 2);
                        break;
                    }
                    //If VARCHAR
                    case "BOOLEAN":
                    case "VARCHAR":
                    case "TIMESTAMPTZ":
                    case "TIMESTAMP": {
                        tableSchemaBuilder.addStringField(definition.getString("COLUMN_NAME"));
                        break;
                    }
                    //If DATETIME
                    case "DATETIME":
                    {
                        tableSchemaBuilder.addDateDayField(definition.getString("COLUMN_NAME"));
                        break;
                    }

                    default:
                        tableSchemaBuilder.addStringField(definition.getString("COLUMN_NAME"));
                }
            }
            return tableSchemaBuilder.build();

        }
        catch(SQLException e)
        {
            throw new RuntimeException("Error in building the table schema: " + e.getMessage(), e);
        }

    }
}
