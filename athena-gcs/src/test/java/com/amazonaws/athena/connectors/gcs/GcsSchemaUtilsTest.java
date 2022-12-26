/*-
 * #%L
 * Amazon Athena GCS Connector
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class})

public class GcsSchemaUtilsTest
{
    @Mock
    StorageMetadata storageMetadata;

    @Test
    public void testBuildTableSchema() throws Exception
    {
        Table table = new Table();
        table.setName("birthday");
        table.setDatabaseName("default");
        table.setParameters(ImmutableMap.of("classification", "parquet"));
        table.setStorageDescriptor(new StorageDescriptor()
                .withLocation("gs://mydatalake1test/birthday/"));
        table.setCatalogId("catalog");
        when(storageMetadata.getStorageTable(Mockito.any(),Mockito.any(),Mockito.any())).thenReturn(Optional.of(GcsTestUtils.getTestSchemaFields()));
        Schema schema = GcsSchemaUtils.buildTableSchema(storageMetadata, table);
        assertNotNull(schema);
    }

}
