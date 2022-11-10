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

import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.datasource.CsvDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*"})
@PrepareForTest({StorageDatasourceFactory.class, GoogleCredentials.class, GcsSchemaUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class})
public class GcsCompositeHandlerTest
{
    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;

    @Mock
    CsvDatasource csvDatasource;

    @Mock
    GoogleCredentials credentials;

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setUp()
    {
        environmentVariables.set("gcs_credential_key", "gcs_credential_keys");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void gcsCompositeHandlerTest() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        PowerMockito.mockStatic(System.class);
        HashMap<String, String> map = new HashMap<>();
        map.put("file_extension", "csv");
        map.put("gcs_secret_name", "gcs_secret_name");
        map.put("gcs_credential_key", "athena_gcs_keys");
        PowerMockito.when(System.getenv("gcs_secret_name")).thenReturn("athena_gcs_key");
        PowerMockito.when(System.getenv()).thenReturn(map);
        PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(List.of("v1")).withSecretString("{\"gcs_credential_keys\": \"test\"}");
        Mockito.when(secretsManager.getSecretValue(Mockito.any())).thenReturn(getSecretValueResult);
        PowerMockito.mockStatic(ServiceAccountCredentials.class);
        PowerMockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        suppress(constructor(AbstractStorageDatasource.class, StorageDatasourceConfig.class));
        PowerMockito.mockStatic(StorageDatasourceFactory.class);
        PowerMockito.when(StorageDatasourceFactory.createDatasource(anyString(), Mockito.any())).thenReturn(csvDatasource);
        PowerMockito.mockStatic(GoogleCredentials.class);
        //suppress(constructor(GcsMetadataHandler.class));
        PowerMockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        new GcsCompositeHandler();
    }
}
