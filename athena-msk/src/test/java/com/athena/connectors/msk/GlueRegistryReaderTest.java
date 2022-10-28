/*-
 * #%L
 * athena-msk
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
package com.athena.connectors.msk;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetSchemaResult;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import com.amazonaws.services.glue.model.ListSchemasResult;
import com.amazonaws.services.glue.model.SchemaListItem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*",
        "jdk.internal.reflect.*", "javax.crypto.*","javax.security.*"})

@PrepareForTest({AWSGlueClientBuilder.class})
public class GlueRegistryReaderTest {
    GlueRegistryReader glueRegistryReader;

    @Mock
    AWSGlue awsGlue;

    @Before
    public void init(){
        System.setProperty("aws.region", "us-east-1");
        MockitoAnnotations.initMocks(this);
    }
    @Test
    public void testgetSchemaListItemsWithSchemaRegistryARN()  {
        String registryNameArn="defaultregistrynamearn";
        List<SchemaListItem> list = new ArrayList<>();
        List<SchemaListItem> expected;
        ListSchemasResult listSchemasResult = new ListSchemasResult();
        SchemaListItem schemaListItem = new SchemaListItem();
        schemaListItem.setSchemaName("defaultschemaname");
        list.add(schemaListItem);
        listSchemasResult.setSchemas(list);
        PowerMockito.when(awsGlue.listSchemas(Mockito.any())).thenReturn(listSchemasResult);
        PowerMockito.mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        glueRegistryReader=new GlueRegistryReader();
        expected=glueRegistryReader.getSchemaListItemsWithSchemaRegistryARN(registryNameArn);
        Assert.assertNotNull(expected);
    }
    @Test
    public void testgetSchemaVersionResult() {
        String arn="defaultarn",schemaname="defaultschemaname",schemaversionid = "defaultversionid";
        Long latestschemaversion =123L;
        GetSchemaVersionResult expected;
        GetSchemaResult getSchemaResult=new GetSchemaResult();
        GetSchemaVersionResult getSchemaVersionResult=new GetSchemaVersionResult();
        getSchemaResult.setSchemaArn(arn);
        getSchemaResult.setSchemaName(schemaname);
        getSchemaResult.setLatestSchemaVersion(latestschemaversion);
        getSchemaVersionResult.setSchemaArn(arn);
        getSchemaVersionResult.setSchemaVersionId(schemaversionid);
        PowerMockito.mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        PowerMockito.when(awsGlue.getSchema(Mockito.any())).thenReturn(getSchemaResult);
        PowerMockito.when(awsGlue.getSchemaVersion(Mockito.any())).thenReturn(getSchemaVersionResult);
        glueRegistryReader=new GlueRegistryReader();
        expected=glueRegistryReader.getSchemaVersionResult(arn);
        Assert.assertNotNull(expected);
    }
}
