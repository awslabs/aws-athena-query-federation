/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.datasource.csv;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.storage.mock.GcsMarker;
import com.amazonaws.athena.storage.mock.GcsConstraints;
import com.amazonaws.athena.storage.mock.GcsReadRecordsRequest;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class CsvFilterTest
{

    public static final String EMPLOYEE_ID = "EMPLOYEEID";
    static CsvFilter csvFilter;
    static FederatedIdentity federatedIdentity;
    private final String[] fields = {EMPLOYEE_ID, "WEEKID", "NAME", "EMAIL", "ADDRESS1", "ADDRESS2", "CITY", "STATE", "ZIPCODE"};

    @BeforeClass
    public static void setUp()
    {
        csvFilter = new CsvFilter();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    public Map<String, ValueSet> createSummary()
    {
        Block block = Mockito.mock(Block.class);

        FieldReader fieldReader = Mockito.mock(FieldReader.class);
        Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(EMPLOYEE_ID, Types.MinorType.VARCHAR.getType()));

        Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
        Marker low = new GcsMarker(block, Marker.Bound.EXACTLY, false).withValue("90");
        return Map.of(
                EMPLOYEE_ID, SortedRangeSet.of(false, new Range(low, low))
        );
    }

    public void addSchemaFields(SchemaBuilder schemaBuilder)
    {
        for (String field : fields) {
            schemaBuilder.addField(FieldBuilder.newBuilder(field,
                    Types.MinorType.VARCHAR.getType()).build());
        }
    }

    @Test
    public void testEvaluator()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        addSchemaFields(schemaBuilder);
        Schema fieldSchema = schemaBuilder.build();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add("testPartitionCol", "testPartitionValue");

        Constraints constraints = new GcsConstraints(createSummary());
        GcsReadRecordsRequest recordsRequest = new GcsReadRecordsRequest(federatedIdentity,
                "default", "testQueryId", new TableName("default", "test"),
                fieldSchema, splitBuilder.build(), constraints, 1024, 1024);

        assertNotNull(csvFilter.evaluator(recordsRequest));
    }

    @Test
    public void testEvaluator2()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        addSchemaFields(schemaBuilder);
        Schema fieldSchema = schemaBuilder.build();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add("testPartitionCol", "testPartitionValue");

        Constraints constraints = new GcsConstraints(createSummary());

        assertNotNull(csvFilter.evaluator(fieldSchema, constraints, new TableName("default", "test"), splitBuilder.build()));
    }


}
