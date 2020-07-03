package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

public class TestBase {
    protected static final FederatedIdentity IDENTITY = new FederatedIdentity("id", "principal", "account");
    protected static final String QUERY_ID = "query_id-" + System.currentTimeMillis();
    protected static final String PARTITION_ID = "partition_id";
    protected static final String DEFAULT_CATALOG = "default";
    protected static final String TEST_TABLE = "airport";
    protected static final String DEFAULT_SCHEMA = "graph-database";
    protected static final String CONNECTION_STRING = "connectionString";
    protected static final TableName TABLE_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE);
}