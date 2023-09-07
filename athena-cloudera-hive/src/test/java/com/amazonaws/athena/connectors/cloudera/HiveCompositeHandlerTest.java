/*-
 * #%L
 * athena-cloudera-hive
 * %%
 * Copyright (C) 2019 - 2020 Amazon web services
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import org.testng.Assert;

@RunWith(MockitoJUnitRunner.class)
public class HiveCompositeHandlerTest
{

    private HiveCompositeHandler hiveCompositeHandler;
    @BeforeClass
    public static void dataSetUP() {
        System.setProperty("aws.region", "us-west-2");
    }
    @Test
    public void HiveCompositeHandlerTest(){
        Exception ex = null;
        try {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1", HiveConstants.HIVE_NAME,
                "hdphive2://jdbc:hive2://54.89.6.2:10000/authena;AuthMech=3;${testSecret}","testSecret");
        Mockito.mockStatic(JDBCUtil.class);
        JDBCUtil tested = Mockito.mock(JDBCUtil.class);
        Mockito.when(tested.getSingleDatabaseConfigFromEnv(HiveConstants.HIVE_NAME, System.getenv())).thenReturn(databaseConnectionConfig);
        new HiveCompositeHandler();
        }catch(Exception e) {
            ex =e;
        }
        Assert.assertEquals(null, ex);
    }

}
