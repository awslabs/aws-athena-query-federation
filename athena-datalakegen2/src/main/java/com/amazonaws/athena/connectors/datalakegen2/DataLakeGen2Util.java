/*-
 * #%L
 * athena-datalakegen2
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.datalakegen2;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants.SQL_POOL;

public class DataLakeGen2Util
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataLakeGen2Util.class);
    
    private DataLakeGen2Util()
    {
    }

    private static final Pattern DATALAKE_CONN_STRING_PATTERN = Pattern.compile("([a-zA-Z]+)://([^;]+);(.*)");

    public static String checkEnvironment(String url)
    {
        if (StringUtils.isBlank(url)) {
            return null;
        }
        
        // checking whether it's Azure serverless environment or not based on host name
        Matcher m = DATALAKE_CONN_STRING_PATTERN.matcher(url);
        String hostName = "";
        if (m.find() && m.groupCount() == 3) {
            hostName = m.group(2);
        }
        
        if (StringUtils.isNotBlank(hostName) && hostName.contains("ondemand")) {
            LOGGER.info("Azure serverless environment detected");
            return SQL_POOL;
        }
        
        return null;
    }
}
