/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.integ;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * This Lambda function handler is only used within the HBase integration tests. The Lambda function,
 * when invoked, will generate a HBase database/namespace, table, and insert values.
 * The invocation of the Lambda function must include the following environment variables:
 * default_hbase - The connection string used to connect to the HBase instance
 * (e.g. ec2-000-000-000-000.compute-1.amazonaws.com:50075:2081).
 * database_name - The HBase database/namespace name.
 * table_name - The HBase table name.
 */
public class HbaseIntegTestHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseIntegTestHandler.class);

    public static final String HANDLER = "com.amazonaws.athena.connectors.hbase.integ.HbaseIntegTestHandler";

    private final String connectionStr;
    private final String databaseName;
    private final String tableName;

    public HbaseIntegTestHandler()
    {
        connectionStr = System.getenv("default_hbase");
        databaseName = System.getenv("database_name");
        tableName = System.getenv("table_name");
    }

    @Override
    public final void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    {
        logger.info("handleRequest - enter");

        try (HbaseTableUtils tableUtils = new HbaseTableUtils(databaseName, tableName, connectionStr)) {
            // Create HBase database/namespace and table.
            tableUtils.createDbAndTable(ImmutableList.of("movie", "info"));
            List<Put> rows = new ImmutableList.Builder<Put>()
                    .add(new HbaseRowBuilder("1")
                            .addColumn("movie", "title", "Interstellar")
                            .addColumn("info", "year", "2014")
                            .addColumn("info", "director", "Christopher Nolan")
                            .addColumn("info", "lead_actor", "Matthew McConaughey")
                            .build())
                    .add(new HbaseRowBuilder("2")
                            .addColumn("movie", "title", "Aliens")
                            .addColumn("info", "year", "1986")
                            .addColumn("info", "director", "James Cameron")
                            .addColumn("info", "lead_actor", "Sigourney Weaver")
                            .build())
                    .build();
            tableUtils.insertRows(rows);
            logger.info("New rows inserted successfully.");
        }
        catch (IOException e) {
            logger.error("Error setting up HBase table: {}", e.getMessage(), e);
        }

        try (HbaseTableUtils tableUtils = new HbaseTableUtils("datatypes", "datatypes_table", connectionStr)) {
            // Create HBase database/namespace and table.
            tableUtils.createDbAndTable(ImmutableList.of("datatype"));
            List<Put> rows = new ImmutableList.Builder<Put>()
                    .add(new HbaseRowBuilder("1")
                            .addColumn("datatype", "int_type", String.valueOf(Integer.MIN_VALUE))
                            .addColumn("datatype", "smallint_type", String.valueOf(Short.MIN_VALUE))
                            .addColumn("datatype", "bigint_type", String.valueOf(Long.MIN_VALUE))
                            .addColumn("datatype", "varchar_type", "John Doe")
                            .addColumn("datatype", "boolean_type", String.valueOf(true))
                            .addColumn("datatype", "float4_type", String.valueOf(1e-37f))
                            .addColumn("datatype", "float8_type", String.valueOf(1e-307))
                            .addColumn("datatype", "date_type", "2013-06-01")
                            .addColumn("datatype", "timestamp_type", "2016-06-22T19:10:25")
                            .build())
                    .build();
            tableUtils.insertRows(rows);
            logger.info("New rows inserted successfully.");
        }
        catch (IOException e) {
            logger.error("Error setting up HBase table: {}", e.getMessage(), e);
        }

        try (HbaseTableUtils tableUtils = new HbaseTableUtils("empty", "empty_table", connectionStr)) {
            // Create HBase database/namespace and table.
            tableUtils.createDbAndTable(ImmutableList.of("empty"));
        }
        catch (IOException e) {
            logger.error("Error setting up HBase table: {}", e.getMessage(), e);
        }

        logger.info("handleRequest - exit");
    }
}
