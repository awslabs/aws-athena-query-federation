/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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

import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;

public class GenericGcsTest
{
    protected MockedStatic<AmazonS3ClientBuilder> mockedS3Builder;
    protected  MockedStatic<AWSSecretsManagerClientBuilder> mockedSecretManagerBuilder;
    protected  MockedStatic<AmazonAthenaClientBuilder> mockedAthenaClientBuilder;
    protected  MockedStatic<GoogleCredentials> mockedGoogleCredentials;
    protected  MockedStatic<GcsUtil> mockedGcsUtil;

    protected MockedStatic<ServiceAccountCredentials> mockedServiceAccountCredentials;

    protected void initCommonMockedStatic()
    {
        mockedS3Builder = Mockito.mockStatic(AmazonS3ClientBuilder.class);
        mockedSecretManagerBuilder = Mockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        mockedAthenaClientBuilder = Mockito.mockStatic(AmazonAthenaClientBuilder.class);
        mockedGoogleCredentials = Mockito.mockStatic(GoogleCredentials.class);
        mockedGcsUtil = Mockito.mockStatic(GcsUtil.class);
        mockedServiceAccountCredentials = Mockito.mockStatic(ServiceAccountCredentials.class);
    }

    protected void closeMockedObjects() {
        mockedS3Builder.close();
        mockedSecretManagerBuilder.close();
        mockedAthenaClientBuilder.close();
        mockedGoogleCredentials.close();
        mockedGcsUtil.close();
        mockedServiceAccountCredentials.close();
    }

    protected static void setInternalState(Object target, String field, Object value) {
        Class<?> c = target.getClass();
        try {
            Field f = getFieldFromHierarchy(c, field);  // Checks superclasses.
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to set internal state on a private field. [...]", e);
        }
    }

    private static Field  getFieldFromHierarchy(Class<?> clazz, String field) {
        Field f = getField(clazz, field);
        while (f == null && clazz != Object.class) {
            clazz = clazz.getSuperclass();
            f = getField(clazz, field);
        }
        if (f == null) {
            throw new RuntimeException(
                    "You want me to get this field: '" + field +
                            "' on this class: '" + clazz.getSimpleName() +
                            "' but this field is not declared withing hierarchy of this class!");
        }
        return f;
    }

    private static Field  getField(Class<?> clazz, String field) {
        try {
            return clazz.getDeclaredField(field);
        } catch (NoSuchFieldException e) {
            return null;
        }
    }
}
