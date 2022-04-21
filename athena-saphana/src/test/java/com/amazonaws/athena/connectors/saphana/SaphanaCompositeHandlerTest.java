/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JDBCUtil.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*"})
public class SaphanaCompositeHandlerTest {

    private SaphanaCompositeHandler saphanaCompositeHandler;

    @Test
    public void SaphanaCompositeHandlerTest(){
        Exception ex = null;
        try {
            DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SaphanaConstants.SAPHANA_NAME,
                    "saphana://jdbc:sap://hostname/?${trianz-saphana-athena-jdbc}");
            PowerMockito.mockStatic(JDBCUtil.class);
            JDBCUtil tested = PowerMockito.mock(JDBCUtil.class);
            PowerMockito.when(tested.getSingleDatabaseConfigFromEnv(SaphanaConstants.SAPHANA_NAME)).thenReturn(databaseConnectionConfig);
            saphanaCompositeHandler = new SaphanaCompositeHandler();
            assertEquals(saphanaCompositeHandler.getClass(),SaphanaCompositeHandler.class);
        }catch (Exception e){
            ex = e;
        }
        assertEquals(null,ex);
    }
}
