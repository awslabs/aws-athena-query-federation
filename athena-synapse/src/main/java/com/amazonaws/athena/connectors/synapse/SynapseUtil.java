/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SynapseUtil
{
    private SynapseUtil()
    {
    }

    private static final Pattern SYNAPSE_CONN_STRING_PATTERN = Pattern.compile("([a-zA-Z]+)://([^;]+);(.*)");

    public static String checkEnvironment(String url)
    {
        // checking whether it's Azure serverless environment or not based on host name
        Matcher m = SYNAPSE_CONN_STRING_PATTERN.matcher(url);
        String hostName = "";
        if (m.find() && m.groupCount() == 3) {
            hostName = m.group(2);
        }
        if (StringUtils.isNotBlank(hostName) && hostName.contains("ondemand")) {
            return "azureServerless";
        }
        return null;
    }
}
