/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb.integ;

import com.amazonaws.athena.connectors.docdb.DocDBConnectionFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * This Lambda function handler is only used within the DocumentDB integration tests. The Lambda function,
 * when invoked, will generate a MongoDB database, table, and insert values.
 * The invocation of the Lambda function must include the following environment variables:
 * default_docdb - The connection string used to connect to the DocumentDB cluster (e.g. mongodb://...).
 * database_name - The MongoDB database name.
 * table_name - The MongoDB collection name.
 */
public class DocDbIntegTestHandler
        implements RequestStreamHandler
{
    public static final String HANDLER = "com.amazonaws.athena.connectors.docdb.integ.DocDbIntegTestHandler";

    private static final Logger logger = LoggerFactory.getLogger(DocDbIntegTestHandler.class);

    private final DocDBConnectionFactory connectionFactory;
    private final String connectionString;
    private final String moviesDatabaseName;
    private final String moviesTableName;
    private final String datatypesTableName;
    private final String datatypesDatabaseName;

    public DocDbIntegTestHandler()
    {
        connectionFactory = new DocDBConnectionFactory();
        connectionString = System.getenv("default_docdb");
        moviesDatabaseName = System.getenv("movies_database_name");
        moviesTableName = System.getenv("movies_table_name");
        datatypesDatabaseName = System.getenv("datatypes_database_name");
        datatypesTableName = System.getenv("datatypes_table_name");
    }

    @Override
    public final void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    {
        logger.info("handleRequest - enter");

        try (MongoClient mongoClient = connectionFactory.getOrCreateConn(connectionString)) {
            mongoClient.getDatabase(moviesDatabaseName)
                    .getCollection(moviesTableName)
                    .insertOne(new Document()
                            .append("_id", "1")
                            .append("title", "The Matrix")
                            .append("year", Integer.valueOf(1999))
                            .append("cast", ImmutableList.of("Keanu Reeves", "Laurence Fishburn", "Carrie-Anne Moss",
                                    "Hugo Weaving")));

            mongoClient.getDatabase(moviesDatabaseName)
                    .getCollection(moviesTableName)
                    .insertOne(new Document()
                            .append("_id", "2")
                            .append("title", "Interstellar")
                            .append("year", Integer.valueOf(2014))
                            .append("cast", ImmutableList.of("Matthew McConaughey", "John Lithgow", "Ann Hathaway",
                                    "David Gyasi", "Michael Caine", "Jessica Chastain", "Matt Damon", "Casey Affleck")));

            // unfortunately, the only way to use the constants would be to link in the integration SDK.
            mongoClient.getDatabase(datatypesDatabaseName)
                    .getCollection(datatypesTableName)
                    .insertOne(new Document()
                            .append("_id", "1")
                            .append("int_type", Integer.MIN_VALUE)
                            .append("smallint_type", Short.MIN_VALUE)
                            .append("bigint_type", Long.MIN_VALUE)
                            .append("varchar_type", "John Doe")
                            .append("boolean_type", true)
                            .append("float4_type", 1E-37f)
                            .append("float8_type", 1E-307)
                            .append("date_type", "2013-06-01")
                            .append("timestamp_type", "2016-06-22T19:10:25")
                            .append("textarray_type", ImmutableList.of("(408)-589-5846", "(408)-589-5555"))
                    );

            mongoClient.getDatabase(datatypesDatabaseName)
                    .getCollection("null_table")
                    .insertOne(new Document()
                                    .append("_id", "1")
                                    .append("int_type", null)
                    );
            logger.info("Documents inserted successfully.");
        }
        catch (Exception e) {
            logger.error("Error setting up MongoDB table {}", e.getMessage(), e);
        }

        logger.info("handleRequest - exit");
    }
}
