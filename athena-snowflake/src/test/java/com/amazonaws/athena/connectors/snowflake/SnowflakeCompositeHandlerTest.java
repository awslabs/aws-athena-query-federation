/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeCompositeHandlerTest
{

    private SnowflakeCompositeHandler snowflakeCompositeHandler;
    @Test
    public void snowflakeCompositeHandlerTest(){
        Exception ex = null;
        try {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1", SnowflakeConstants.SNOWFLAKE_NAME,
                "snowflake://jdbc:snowflake://hostname/?warehouse=warehousename&db=dbname&schema=schemaname&user=xxx&password=xxx");
        Mockito.mockStatic(JDBCUtil.class);
        JDBCUtil tested = Mockito.mock(JDBCUtil.class);
        Mockito.when(tested.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, System.getenv())).thenReturn(databaseConnectionConfig);
        snowflakeCompositeHandler = new SnowflakeCompositeHandler();
        assertEquals(snowflakeCompositeHandler.getClass(),SnowflakeCompositeHandler.class);
        }catch (Exception e){
            ex = e;
        }
        assertEquals(null,ex);
    }
}
