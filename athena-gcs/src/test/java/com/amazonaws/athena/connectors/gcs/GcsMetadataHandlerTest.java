/*-
 * #%L
 * athena-gcs
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*", "javax.security.*"})
@PrepareForTest({StorageDatasourceFactory.class, GoogleCredentials.class, GcsSchemaUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class, AWSGlueClientBuilder.class, GlueMetadataHandler.class})
public class GcsMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final String TEST_TOKEN = "testToken";
    private static final String SCHEMA_NAME = "default";
    private GcsMetadataHandler gcsMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    @Mock
    private AWSGlue awsGlue;

    @Mock
    GoogleCredentials credentials;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;

    @Mock
    AmazonS3 amazonS3;

    @Mock
    private AmazonAthena athena;

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setUp() throws Exception
    {
        environmentVariables.set("gcs_credential_key", "gcs_credential_keys");
        PowerMockito.mockStatic(ServiceAccountCredentials.class);
        PowerMockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);

        PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(List.of("v1")).withSecretString("{\"gcs_credential_keys\": \"test\"}");
        Mockito.when(secretsManager.getSecretValue(Mockito.any())).thenReturn(getSecretValueResult);
        PowerMockito.mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", amazonS3, awsGlue);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void testDoListSchemaNames() throws Exception
    {
        GetDatabasesResult result = new GetDatabasesResult().withDatabaseList(
                new Database().withName("gcsdatabase").withLocationUri("s3://gcs"),
                new Database().withName("s3database").withLocationUri("s3://gcs"));
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, CATALOG);
        PowerMockito.when(awsGlue.getDatabases(any())).thenReturn(result);
        ListSchemasResponse schemaNamesResponse = gcsMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        List<String> expectedSchemaNames = new ArrayList<>();
        expectedSchemaNames.add("gcsdatabase");
        expectedSchemaNames.add("s3database");
        assertEquals(expectedSchemaNames, new ArrayList<>(schemaNamesResponse.getSchemas()));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesThrowsException() throws Exception
    {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        when(listSchemasRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListSchemasResponse listSchemasResponse = gcsMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(listSchemasResponse);
    }

    @Test
    public void testDoListTables() throws Exception
    {
        GetTablesResult getTablesResult = new GetTablesResult();
        List<Table> tableList = new ArrayList<>();
        tableList.add(new Table().withName("testtable1")
                .withParameters(ImmutableMap.of("classification", "parquet"))
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("gs://default/testtable1/")));
        tableList.add(new Table().withName("testtable2")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("gs://default/testtable2/")
                        .withParameters(ImmutableMap.of("classification", "parquet"))));
        getTablesResult.setTableList(tableList);
        PowerMockito.when(awsGlue.getTables(any())).thenReturn(getTablesResult);
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, QUERY_ID, CATALOG, SCHEMA_NAME, TEST_TOKEN,50 );
        ListTablesResponse tableNamesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertEquals(2, tableNamesResponse.getTables().size());
    }

    @Test(expected = RuntimeException.class)
    public void testDoListTablesThrowsException() throws Exception
    {
        ListTablesRequest listTablesRequest = mock(ListTablesRequest.class);
        when(listTablesRequest.getCatalogName()).thenThrow(new RuntimeException("RunTimeException() "));
        ListTablesResponse listTablesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertNull(listTablesResponse);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "parquet");
        Schema schema = new Schema(asList(field), metadataSchema);
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "gcs", new TableName(SCHEMA_NAME, "testtable"));
        Table table = new Table();
        table.setName("testtable");
        table.setDatabaseName("default");
        table.setParameters(ImmutableMap.of("classification", "parquet"));
        table.setStorageDescriptor(new StorageDescriptor()
                .withLocation("gs://default/testtable/").withColumns(new Column()));
        table.setCatalogId(CATALOG);
        GetTableResult getTableResult = new GetTableResult();
        getTableResult.setTable(table);
        PowerMockito.when(awsGlue.getTable(any())).thenReturn(getTableResult);
        PowerMockito.mockStatic(GcsSchemaUtils.class);
        PowerMockito.when(GcsSchemaUtils.buildTableSchema(any(),any())).thenReturn(schema);
        GetTableResponse res = gcsMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        Field expectedField = res.getSchema().findField("name");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(expectedField.getType()));
    }
}
