/*-
 * #%L
 * athena-clickhouse
 * %%
 * Copyright (C) 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.clickhouse;

import java.util.Collections;
import java.util.Map;

/**
 * Clickhouse Module Constants including name, driver, and default port.
 */
public final class ClickHouseConstants
{
    public static final String NAME = "clickhouse";
    public static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";
    public static final int DEFAULT_PORT = 8123;
    // "cc.blynk.clickhouse.ClickHouseDriver"
    // "com.github.housepower.jdbc.ClickHouseDriver";
    // "com.clickhouse.jdbc.ClickHouseDriver";
    // "com.mysql.cj.jdbc.Driver"
    // https://clickhouse.com/docs/en/guides/sre/network-ports
    // 8123; // Http
    // 9000; // Native-TCP
    // 9004; // MySQL
    static final Map<String, String> JDBC_PROPERTIES = Collections.singletonMap("databaseTerm", "SCHEMA");

    private ClickHouseConstants() {}
}
