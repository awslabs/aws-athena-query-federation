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

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class SynapseCompositeHandlerTest {

    private SynapseCompositeHandler synapseCompositeHandler;
    private static final Logger logger = LoggerFactory.getLogger(SynapseCompositeHandlerTest.class);

    static {
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void synapseCompositeHandlerTest() {
        Exception ex = null;
        try {
            DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1", SynapseConstants.NAME,
                    "synapse://jdbc:sqlserver://hostname;databaseName=fakedatabase;");
            Mockito.mockStatic(JDBCUtil.class);
            JDBCUtil tested = Mockito.mock(JDBCUtil.class);
            Mockito.when(tested.getSingleDatabaseConfigFromEnv(SynapseConstants.NAME, System.getenv())).thenReturn(databaseConnectionConfig);
            synapseCompositeHandler = new SynapseCompositeHandler();
            logger.info("synapseCompositeHandler: {}", synapseCompositeHandler);
        } catch (Exception e){
            ex = e;
        }
        Assert.assertEquals(null,ex);
    }
}
