/*-
 * #%L
 * athena-tpcds
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.tpcds.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TPCDSQueryPassthrough implements QueryPassthroughSignature
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(TPCDSQueryPassthrough.class);

        // Constant value representing the name of the query.
        public static final String NAME = "query";

        // Constant value representing the domain of the query.
        public static final String SCHEMA_NAME = "system";

        public static final String TPCDS_CATALOG = "TPCDS_CATALOG";
        public static final String TPCDS_SCHEMA = "TPCDS_SCHEMA";
        public static final String TPCDS_TABLE = "TPCDS_TABLE";

        @Override
        public String getFunctionSchema()
        {
            return SCHEMA_NAME;
        }

        @Override
        public String getFunctionName()
        {
            return NAME;
        }

        @Override
        public List<String> getFunctionArguments()
        {
            return Arrays.asList(TPCDS_CATALOG, TPCDS_SCHEMA, TPCDS_TABLE);
        }

        @Override
        public Logger getLogger()
        {
            return LOGGER;
        }
    }
