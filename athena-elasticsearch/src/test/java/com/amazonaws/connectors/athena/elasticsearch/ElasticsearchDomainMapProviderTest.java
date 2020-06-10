/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.connectors.athena.elasticsearch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This class is used to test the ElasticsearchDomainMapProvider class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchDomainMapProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchDomainMapProviderTest.class);

    /**
     * Tests that a domain mapping string is parsed correctly into a domain map containing domain names and their
     * associated endpoints.
     */
    @Test
    public void getDomainMapTest()
    {
        logger.info("getDomainMapTest - enter");

        ElasticsearchDomainMapProvider domainMapProvider = new ElasticsearchDomainMapProvider(false);
        String domainMapping = "domain1=myusername@password:www.endpoint1.com,domain2=myusername@password:www.endpoint2.com";
        Map<String, String> domainMap = domainMapProvider.getDomainMap(domainMapping);

        logger.info("Domain map: {}", domainMap);

        assertEquals("Invalid number of elements in map:", 2, domainMap.size());
        assertEquals("Invalid value for domain1:", "myusername@password:www.endpoint1.com",
                domainMap.get("domain1"));
        assertEquals("Invalid value for domain2:", "myusername@password:www.endpoint2.com",
                domainMap.get("domain2"));

        logger.info("getDomainMapTest - exit");
    }
}
