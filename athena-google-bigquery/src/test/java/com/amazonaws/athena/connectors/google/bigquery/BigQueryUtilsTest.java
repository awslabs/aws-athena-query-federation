/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryUtilsTest
{
    @Mock
    BigQuery bigQuery;
    BigQueryPage<Dataset> datasets;
    BigQueryPage<Table> tables;
    String datasetName;
    @Mock
    private SecretsManagerClient secretsManagerClient;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;
    private MockedStatic<ServiceAccountCredentials> mockedServiceAccountCredentials;
    private MockedStatic<SecretsManagerClient> mockedAWSSecretsManagerClientBuilder;
    private Map<String, String> configOptions;
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    private static final String FIELD_NAME = "test";
    private static final String DATASET_NAME = "dataset1";
    private static final String CREDS_SM_NAME = "test-secret-id";

    @Before
    public void init()
    {
        System.setProperty("aws.region", "us-east-1");

        //Mock the BigQuery Client to return Datasets, and Table Schema information.
        datasets = new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, 2));
        when(bigQuery.listDatasets(nullable(String.class))).thenReturn(datasets);
        //Get the first dataset name.
        datasetName = datasets.iterateAll().iterator().next().getDatasetId().getDataset();
        tables = new BigQueryPage<>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME, DATASET_NAME, 2));
        when(bigQuery.listTables(nullable(DatasetId.class))).thenReturn(tables);
        configOptions = new HashMap<>();
        mockedServiceAccountCredentials = mockStatic(ServiceAccountCredentials.class);
        mockedAWSSecretsManagerClientBuilder = mockStatic(SecretsManagerClient.class);
    }

    @After
    public void tearDown()
    {
        mockedServiceAccountCredentials.close();
        mockedAWSSecretsManagerClientBuilder.close();
        rootAllocator.close();
    }

    @Test
    public void testFixCaseForTableName()
    {
        final String expectedTableName = "table1";
        String result = BigQueryUtils.fixCaseForTableName(
                BigQueryTestUtils.PROJECT_1_NAME, datasetName, expectedTableName, bigQuery);
        assertEquals(expectedTableName, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFixCaseForTableNameNotFound()
    {
        BigQueryUtils.fixCaseForTableName(
                BigQueryTestUtils.PROJECT_1_NAME, datasetName, "nonexistent", bigQuery);
    }

    @Test
    public void testGetCredentialsFromSecretsManager() throws IOException {
        configOptions.put(BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID, CREDS_SM_NAME);
        String secretJson = "{ \"type\": \"service_account\", \"project_id\": \"test-project\" }";
        GetSecretValueResponse secretResponse = GetSecretValueResponse.builder()
                .secretString(secretJson)
                .build();

        when(SecretsManagerClient.create()).thenReturn(secretsManagerClient);
        when(secretsManagerClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(secretResponse);
        when(ServiceAccountCredentials.fromStream(any())).thenReturn(serviceAccountCredentials);
        when(serviceAccountCredentials.createScoped((Collection<String>) any()))
                .thenReturn(serviceAccountCredentials);

        Credentials result = BigQueryUtils.getCredentialsFromSecretsManager(configOptions);

        assertNotNull(result);
        verify(secretsManagerClient).getSecretValue(any(GetSecretValueRequest.class));
        verify(serviceAccountCredentials).createScoped(eq(ImmutableSet.of(
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/drive")));
    }

    @Test
    public void testTranslateToArrowType()
    {
        assertEquals(new ArrowType.Bool(), BigQueryUtils.translateToArrowType(LegacySQLTypeName.BOOLEAN));
        assertEquals(new ArrowType.Int(64, true), BigQueryUtils.translateToArrowType(LegacySQLTypeName.INTEGER));
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                BigQueryUtils.translateToArrowType(LegacySQLTypeName.FLOAT));
        assertEquals(new ArrowType.Decimal(38, 9, 128), BigQueryUtils.translateToArrowType(LegacySQLTypeName.NUMERIC));
        assertEquals(new ArrowType.Binary(), BigQueryUtils.translateToArrowType(LegacySQLTypeName.BYTES));
        assertEquals(new ArrowType.Struct(), BigQueryUtils.translateToArrowType(LegacySQLTypeName.RECORD));
        assertEquals(new ArrowType.Date(DateUnit.DAY), BigQueryUtils.translateToArrowType(LegacySQLTypeName.DATE));
        assertEquals(new ArrowType.Date(DateUnit.MILLISECOND),
                BigQueryUtils.translateToArrowType(LegacySQLTypeName.DATETIME));
        assertEquals(new ArrowType.Utf8(), BigQueryUtils.translateToArrowType(LegacySQLTypeName.STRING));
    }

    @Test
    public void testGetChildFieldList()
    {
        Field simpleField = Field.of(FIELD_NAME, LegacySQLTypeName.STRING);
        List<org.apache.arrow.vector.types.pojo.Field> result = BigQueryUtils.getChildFieldList(simpleField);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(FIELD_NAME, result.get(0).getName());
        assertEquals(new ArrowType.Utf8(), result.get(0).getType());
    }

    @Test
    public void testGetChildFieldListWithNestedStruct() {
        List<Field> nestedFields = new ArrayList<>();
        nestedFields.add(Field.of("nested1", LegacySQLTypeName.STRING));
        nestedFields.add(Field.of("nested2", LegacySQLTypeName.INTEGER));
        String expectedNestedField = "nested";
        Field nestedStruct = Field.of(expectedNestedField, LegacySQLTypeName.RECORD, nestedFields.toArray(new Field[0]));
        List<Field> fields = new ArrayList<>();
        fields.add(Field.of(FIELD_NAME, LegacySQLTypeName.STRING));
        fields.add(nestedStruct);

        Field structField = Field.of("struct", LegacySQLTypeName.RECORD, fields.toArray(new Field[0]));

        List<org.apache.arrow.vector.types.pojo.Field> result = BigQueryUtils.getChildFieldList(structField);
        assertNotNull(result);
        assertEquals(2, result.size());

        // Check the nested struct field
        org.apache.arrow.vector.types.pojo.Field nestedField = result.get(1);
        assertEquals(expectedNestedField, nestedField.getName());
        assertInstanceOf(ArrowType.Struct.class, nestedField.getType());
        assertEquals(2, nestedField.getChildren().size());
    }

    @Test
    public void testCoerceWithTimeVector() {
        try (TimeMilliVector time = new TimeMilliVector("time", rootAllocator)) {
            // Test coercing a Long value
            Long timeInMillis = 3600000L; // 1 hour in milliseconds
            Object result = BigQueryUtils.coerce(time, timeInMillis);
            assertNotNull(result);
            assertInstanceOf(String.class, result);
            assertEquals("01:00", result);

            // Test coercing a non-Long value
            String timeString = "01:00";
            result = BigQueryUtils.coerce(time, timeString);
            assertNotNull(result);
            assertInstanceOf(String.class, result);
            assertEquals(timeString, result);
        }
    }

    @Test
    public void testCoerceWithTimestampVector() {
        org.apache.arrow.vector.types.pojo.Field timestampField = new org.apache.arrow.vector.types.pojo.Field(
                "timestamp",
                new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null),
                null
        );
        try (TimeStampMilliTZVector timestampVector = new TimeStampMilliTZVector(timestampField, rootAllocator)) {
            // Test coercing a Long value (epoch milliseconds)
            Long timestampInMillis = 1609459200000L; // 2021-01-01 00:00:00 UTC
            Object result = BigQueryUtils.coerce(timestampVector, timestampInMillis);
            assertNotNull(result);
            assertInstanceOf(String.class, result);
            assertEquals("2021-01-01T00:00", result.toString());

            // Test coercing a non-Long value
            String timestampString = "2021-01-01T00:00:00";
            result = BigQueryUtils.coerce(timestampVector, timestampString);
            assertNotNull(result);
            assertEquals(timestampString, result);
        }
    }

    @Test
    public void testCoerceWithOtherVectorTypes_ReturnsSameAsInput() {
        try (IntVector intVector = new IntVector("int", rootAllocator)) {
            Integer expectedValue = 42;
            Object result = BigQueryUtils.coerce(intVector, expectedValue);
            assertEquals(expectedValue, result);
        }
    }

    @Test
    public void testGetObjectFromFieldValueWithDate() throws ParseException {
        FieldValue dateValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-01-01");
        org.apache.arrow.vector.types.pojo.Field dateField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Date(DateUnit.DAY));

        Object result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, dateValue, dateField, false);
        assertNotNull(result);
        assertInstanceOf(Long.class, result);
        assertEquals(19358L, result);
    }

    @Test
    public void testGetObjectFromFieldValueWithDateTime() throws ParseException {
        FieldValue dateTimeValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-01-01T12:00:00");
        org.apache.arrow.vector.types.pojo.Field dateTimeField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Date(DateUnit.MILLISECOND));

        Object result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, dateTimeValue, dateTimeField, false);
        assertNotNull(result);
        assertInstanceOf(Date.class, result);

        dateTimeValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-01-01T12:00:00.000000");
        dateTimeField = org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Date(DateUnit.MILLISECOND));

        result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, dateTimeValue, dateTimeField, false);
        assertNotNull(result);
        assertInstanceOf(LocalDateTime.class, result);
        assertEquals(LocalDateTime.of(2023, 1, 1, 12, 0, 0), result);

    }

    @Test
    public void testGetObjectFromFieldValueWithTimestamp() throws ParseException {
        FieldValue timestampValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "12345");
        org.apache.arrow.vector.types.pojo.Field timestampField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null));

        Object result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, timestampValue, timestampField, true);
        assertNotNull(result);
        assertInstanceOf(Long.class, result);
        assertEquals(12345000L, result);
    }

    @Test
    public void testGetObjectFromFieldValueWithDecimal() throws ParseException {
        FieldValue decimalValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123.45");
        org.apache.arrow.vector.types.pojo.Field decimalField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Decimal(38, 9, 128));

        Object result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, decimalValue, decimalField, false);
        assertNotNull(result);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(new BigDecimal("123.45"), result);
    }

    @Test
    public void testGetObjectFromFieldValueVarcharWithTimestamp() throws ParseException {
        String timestampMillis = "1748604000000"; // Represents 2025-05-30T12:00:00Z
        FieldValue varcharTimestampValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, timestampMillis);
        org.apache.arrow.vector.types.pojo.Field varcharTimestampField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Utf8());
        Object result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, varcharTimestampValue, varcharTimestampField, false);
        assertNotNull(result);
        assertInstanceOf(String.class, result);
        assertEquals(timestampMillis, result);

        result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, varcharTimestampValue, varcharTimestampField, true);
        assertNotNull(result);
        assertInstanceOf(Instant.class, result);
    }

    @Test
    public void testGetObjectFromFieldValueWithNull() throws ParseException {
        FieldValue nullValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, null);
        org.apache.arrow.vector.types.pojo.Field field =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Utf8());
        Object result = BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, nullValue, field, false);
        assertNull(result);
    }

    @Test
    public void testGetComplexObjectFromFieldValueWithStruct() throws ParseException {
        List<com.google.cloud.bigquery.Field> testSchemaFields = List.of(
                Field.of(FIELD_NAME, LegacySQLTypeName.STRING));
        List<FieldValue> bigQueryRowValue = List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test"));
        FieldValueList fieldValueList = FieldValueList.of(bigQueryRowValue,
                FieldList.of(testSchemaFields));
        FieldValue structValue = FieldValue.of(FieldValue.Attribute.RECORD, fieldValueList);
        org.apache.arrow.vector.types.pojo.Field structField =
                org.apache.arrow.vector.types.pojo.Field.nullable("struct", new ArrowType.Struct());

        org.apache.arrow.vector.types.pojo.Field childField =
                org.apache.arrow.vector.types.pojo.Field.nullable("element", new ArrowType.Utf8());
        List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
        children.add(childField);
        structField = new org.apache.arrow.vector.types.pojo.Field(FIELD_NAME, structField.getFieldType(), children);
        Object result = BigQueryUtils.getComplexObjectFromFieldValue(structField, structValue, false);
        assertInstanceOf(Map.class, result);
        Map<Object, Object> structResult = (Map<Object, Object>) result;
        assertEquals(1, structResult.size());
        assertEquals(FIELD_NAME, structResult.get("element"));
    }

    @Test
    public void testGetComplexObjectFromFieldValueWithList() throws ParseException {
        String expectedValue1 = "test1";
        String expectedValue2 = "test2";
        List<FieldValue> repeatedValues = new ArrayList<>();
        repeatedValues.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, expectedValue1));
        repeatedValues.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, expectedValue2));

        FieldValue listValue = FieldValue.of(FieldValue.Attribute.REPEATED, repeatedValues);
        org.apache.arrow.vector.types.pojo.Field listField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.List());
        org.apache.arrow.vector.types.pojo.Field childField =
                org.apache.arrow.vector.types.pojo.Field.nullable("element", new ArrowType.Utf8());
        List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
        children.add(childField);
        listField = new org.apache.arrow.vector.types.pojo.Field(FIELD_NAME, listField.getFieldType(), children);
        Object result = BigQueryUtils.getComplexObjectFromFieldValue(listField, listValue, false);
        assertInstanceOf(List.class, result);
        List<?> listResult = (List<?>) result;
        assertEquals(2, listResult.size());
        assertEquals(expectedValue1, listResult.get(0));
        assertEquals(expectedValue2, listResult.get(1));
    }

    @Test(expected = ParseException.class)
    public void testGetObjectFromFieldValueInvalidDate() throws ParseException {
        FieldValue invalidDateValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "invalid-date");
        org.apache.arrow.vector.types.pojo.Field dateField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, new ArrowType.Date(DateUnit.DAY));

        BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, invalidDateValue, dateField, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetObjectFromFieldValueUnknownType() throws ParseException {
        FieldValue testValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, FIELD_NAME);
        // Create a custom ArrowType that's not handled
        ArrowType customType = new ArrowType.Null();
        org.apache.arrow.vector.types.pojo.Field customField =
                org.apache.arrow.vector.types.pojo.Field.nullable(FIELD_NAME, customType);

        BigQueryUtils.getObjectFromFieldValue(FIELD_NAME, testValue, customField, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bigQueryUtils()
    {
        String newDatasetName = BigQueryUtils.fixCaseForDatasetName(BigQueryTestUtils.PROJECT_1_NAME, "testDataset", bigQuery);
        assertNull(newDatasetName);

        String tableName = BigQueryUtils.fixCaseForTableName(BigQueryTestUtils.PROJECT_1_NAME, datasetName, "test", bigQuery);
        assertNull(tableName);
    }

    @Test
    public void testGetCredentialsFromSecret() {
        try {
            BigQueryUtils.getCredentialsFromSecret("invalid-secret");
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testGetBigQueryClientWithSecret() {
        java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
                BigQueryConstants.GCP_PROJECT_ID, "test"
        );
        try {
            BigQueryUtils.getBigQueryClient(configOptions, "invalid-secret");
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testGetEnvBigQueryCredsSmId()
    {
        java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
                BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID, "test-secret-id"
        );
        String result = BigQueryUtils.getEnvBigQueryCredsSmId(configOptions);
        assertNotNull(result);
    }

    @Test(expected = RuntimeException.class)
    public void testGetEnvBigQueryCredsSmIdEmpty()
    {
        java.util.Map<String, String> configOptions = new java.util.HashMap<>();
        BigQueryUtils.getEnvBigQueryCredsSmId(configOptions);
    }

    @Test
    public void testFixCaseForDatasetNameSuccess()
    {
        String result = BigQueryUtils.fixCaseForDatasetName(BigQueryTestUtils.PROJECT_1_NAME, datasetName, bigQuery);
        assertNotNull(result);
    }
}
