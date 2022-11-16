/*-
 * #%L
 * athena-deltalake
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.connectors.athena.deltalake.converter.DeltaConverter;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_FILE_PROPERTY;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_PARTITION_VALUES_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DeltalakeRecordHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandlerTest.class);
    @Rule
    public TestName testName = new TestName();
    private DeltalakeRecordHandler handler;
    private BlockAllocatorImpl allocator;

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        String dataBucket = "test-bucket-1";

        allocator = new BlockAllocatorImpl();

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", S3_ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.access.key", "NO_NEED");
        conf.set("fs.s3a.secret.key", "NO_NEED");

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(
            S3_ENDPOINT,
            S3_REGION
        );
        amazonS3 = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
        this.handler = new DeltalakeRecordHandler(
            amazonS3,
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            conf,
            dataBucket
        );
    }

    @Test
    public void doReadRecordsNoSpillOnSimpleExample()
        throws Exception
    {
        String isValidCol = "is_valid";
        String regionCol = "region";
        String eventDateCol = "event_date";

        Schema schemaForRead = SchemaBuilder.newBuilder()
            .addBitField(isValidCol)
            .addStringField(regionCol)
            .addDateDayField(eventDateCol)
            .addIntField("amount")
            .addDecimalField("amount_decimal", 10, 2)
            .build();

        String catalogName = "catalog";
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        String queryId = "queryId-" + System.currentTimeMillis();

        ReadRecordsRequest request = new ReadRecordsRequest(
            fakeIdentity(),
            catalogName,
            queryId,
            new TableName("test-database-2", "partitioned-table"),
            schemaForRead,
            Split.newBuilder(makeSpillLocation(queryId, "1234"), null)
                .add(
                    SPLIT_PARTITION_VALUES_PROPERTY,
                    "{\"is_valid\":\"true\",\"region\":\"asia\",\"event_date\":\"2020-12-21\"}"
                )
                .add(
                    SPLIT_FILE_PROPERTY,
                    "is_valid=true/region=asia/event_date=2020-12-21/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-c000.snappy.parquet"
                )
                .build(),
            new Constraints(constraintsMap),
            100_000_000_000L, //100GB don't expect this to spill
            100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        Block expectedBlock = allocator.createBlock(schemaForRead);
        BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 0, true);
        BlockUtils.setValue(expectedBlock.getFieldVector("region"), 0, "asia");
        BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 0, LocalDate.of(2020, 12, 21));
        BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 0, 100);
        BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 0, null);

        BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 0, true);
        BlockUtils.setValue(expectedBlock.getFieldVector("region"), 0, "asia");
        BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 0, LocalDate.of(2020, 12, 21));
        BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 0, 100);
        BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 0, null);

        BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 1, true);
        BlockUtils.setValue(expectedBlock.getFieldVector("region"), 1, "asia");
        BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 1, LocalDate.of(2020, 12, 21));
        BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 1, 200);
        BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 1, null);

        BlockUtils.setValue(expectedBlock.getFieldVector("is_valid"), 2, true);
        BlockUtils.setValue(expectedBlock.getFieldVector("region"), 2, "asia");
        BlockUtils.setValue(expectedBlock.getFieldVector("event_date"), 2, LocalDate.of(2020, 12, 21));
        BlockUtils.setValue(expectedBlock.getFieldVector("amount"), 2, 350);
        BlockUtils.setValue(expectedBlock.getFieldVector("amount_decimal"), 2, null);

        expectedBlock.setRowCount(3);

        ReadRecordsResponse expectedResponse = new ReadRecordsResponse(catalogName, expectedBlock);

        assertEquals(3, response.getRecords().getRowCount());
        assertEquals(expectedResponse, response);
    }

    @Test
    public void doReadRecordsNoSpillOnComplexExample()
        throws Exception
    {
        doReadRecordsNoSpillComplexExampleTest(
            "part-00000-f5930883-53cf-4c59-9a37-2dd49ae799fa.c000.snappy.parquet",
            List.of(
                "[tenant : testTenant0], [testArrayString : {foo,bar}], [testArrayInt : {0,1,2,3}], [testMapString : {[key : foo],[value : bar]}{[key : foo1],[value : bar1]}], [testStruct : {[name : foobar],[id : 0]}], [complexStruct : {[testStruct : {[name : foobar],[id : 4]}],[testArray : {foo,bar}],[testMap : {[key : foo],[value : bar]}{[key : foo1],[value : bar1]}]}], [arrayOfIntArrays : {{0,1,2},{2,3,4}}], [arrayOfStructs : {{[name : foobar],[id : 1]},{[name : foobar1],[id : 2]}}], [arrayOfMaps : {{[key : foo],[value : bar]}{[key : foo1],[value : bar1]},{[key : foo1],[value : bar1]}{[key : foo2],[value : bar3]}}], [mapWithMaps : {[key : boo],[value : {[key : foo],[value : bar]}{[key : foo1],[value : bar1]}]}], [mapWithStructs : {[key : boo],[value : {[name : foobar],[id : 3]}]}], [mapWithArrays : {[key : boo],[value : {foo,bar}]}]",
                "[tenant : testTenant0], [testArrayString : {foo,zar}], [testArrayInt : {0,1,2,3,4}], [testMapString : {[key : foo],[value : zar]}{[key : foo1],[value : zar1]}], [testStruct : {[name : foozar],[id : 10]}], [complexStruct : {[testStruct : {[name : foozar],[id : 14]}],[testArray : {foo,zar}],[testMap : {[key : foo],[value : zar]}{[key : foo1],[value : zar1]}]}], [arrayOfIntArrays : {{10,11,12},{12,13,14}}], [arrayOfStructs : {{[name : foozar],[id : 11]},{[name : foozar1],[id : 12]}}], [arrayOfMaps : {{[key : foo],[value : zar]}{[key : foo1],[value : zar1]},{[key : foo1],[value : zar1]}{[key : foo2],[value : zar3]}}], [mapWithMaps : {[key : boo],[value : {[key : foo],[value : zar]}{[key : foo1],[value : zar1]}]}], [mapWithStructs : {[key : boo],[value : {[name : foozar],[id : 13]}]}], [mapWithArrays : {[key : boo],[value : {foo,zar}]}]"
            )
        );
    }

    @Test
    public void doReadRecordsNoSpillOnComplexExampleWithNulls()
        throws Exception
    {
        doReadRecordsNoSpillComplexExampleTest(
            "part-00000-a0223682-fac8-41e3-bee0-839048a3d60e.c000.snappy.parquet",
            List.of(
                "[tenant : testTenant0], [testArrayString : {}], [testArrayInt : {}], [testMapString : ], [testStruct : {[name : null],[id : null]}], [complexStruct : {[testStruct : {[name : null],[id : null]}],[testArray : {}],[testMap : ]}], [arrayOfIntArrays : {{},{}}], [arrayOfStructs : {{[name : null],[id : null]},{[name : null],[id : null]}}], [arrayOfMaps : {}], [mapWithMaps : {[key : boo],[value : ]}], [mapWithStructs : {[key : boo],[value : {[name : null],[id : null]}]}], [mapWithArrays : {[key : boo],[value : {}]}]"
            )
        );
    }

    @Test
    public void doReadRecordsNoSpillOnComplexExampleWithEmptyValues()
        throws Exception
    {
        doReadRecordsNoSpillComplexExampleTest(
            "part-00000-b3adbd69-254f-4c10-b7db-c9103f11ddb1.c000.snappy.parquet",
            List.of(
                "[tenant : testTenant0], [testArrayString : {}], [testArrayInt : {}], [testMapString : ], [testStruct : {[name : ],[id : 0]}], [complexStruct : {[testStruct : {[name : null],[id : null]}],[testArray : {}],[testMap : ]}], [arrayOfIntArrays : {{},{}}], [arrayOfStructs : {{[name : null],[id : null]},{[name : null],[id : null]}}], [arrayOfMaps : {}], [mapWithMaps : {[key : boo],[value : ]}], [mapWithStructs : {[key : boo],[value : {[name : null],[id : null]}]}], [mapWithArrays : {[key : boo],[value : {}]}]"
            )
        );
    }

    private void doReadRecordsNoSpillComplexExampleTest(String parquetFile, List<String> expectedRows)
        throws Exception
    {
        File schemaStringFile = new File(getClass().getClassLoader()
            .getResource("complex_table_schema.json")
            .getFile());
        String schemaString = FileUtils.readFileToString(schemaStringFile, "UTF-8");
        Schema schemaForRead = DeltaConverter.getArrowSchema(schemaString);
        String catalogName = "catalog";
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        String queryId = "queryId-" + System.currentTimeMillis();

        ReadRecordsRequest request = new ReadRecordsRequest(
            fakeIdentity(),
            catalogName,
            queryId,
            new TableName("test-database-2", "complex-table"),
            schemaForRead,
            Split.newBuilder(makeSpillLocation(queryId, "1234"), null)
                .add(SPLIT_PARTITION_VALUES_PROPERTY, "{\"tenant\":\"testTenant0\"}")
                .add(
                    SPLIT_FILE_PROPERTY,
                    "tenant=testTenant0/" + parquetFile
                )
                .build(),
            new Constraints(constraintsMap),
            100_000_000_000L, //100GB don't expect this to spill
            100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        IntStream
            .range(0, expectedRows.size())
            .forEach(ind -> assertEquals(expectedRows.get(ind), BlockUtils.rowToString(response.getRecords(), ind)));
        assertEquals(expectedRows.size(), response.getRecords().getRowCount());
    }
}
