/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

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
public class Db2CompositeHandlerTest
{
    private Db2CompositeHandler db2CompositeHandler;
    private static final Logger logger = LoggerFactory.getLogger(Db2CompositeHandler.class);

    static {
        System.setProperty("aws.region", "us-east-1");
    }
    @Test
    public void Db2CompositeHandlerTest() {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1", Db2Constants.NAME,
                "dbtwo://jdbc:db2://hostname:50001/dummydatabase:user=dummyuser;password=dummypwd");
        Mockito.mockStatic(JDBCUtil.class);
        Mockito.when(JDBCUtil.getSingleDatabaseConfigFromEnv(Db2Constants.NAME, System.getenv())).thenReturn(databaseConnectionConfig);
        db2CompositeHandler = new Db2CompositeHandler();
        logger.info("Db2CompositeHandler: {}", db2CompositeHandler);
        Assert.assertTrue(db2CompositeHandler instanceof Db2CompositeHandler);
    }
}
