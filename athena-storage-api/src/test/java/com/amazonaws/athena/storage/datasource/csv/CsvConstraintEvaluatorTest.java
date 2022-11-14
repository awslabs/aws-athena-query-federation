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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.storage.mock.AthenaConstraints;
import com.amazonaws.athena.storage.mock.AthenaReadRecordsRequest;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import static com.amazonaws.athena.storage.gcs.GcsTestBase.CSV_FILE;
import static com.amazonaws.athena.storage.datasource.csv.CsvFilterTest.federatedIdentity;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class CsvConstraintEvaluatorTest
{
    static ConstraintEvaluator evaluator;

    @BeforeClass
    public static void setUp()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        new CsvFilterTest().addSchemaFields(schemaBuilder);
        Schema fieldSchema = schemaBuilder.build();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add("testPartitionCol", "testPartitionValue");

        Constraints constraints = new AthenaConstraints(new CsvFilterTest().createSummary());
        AthenaReadRecordsRequest recordsRequest = new AthenaReadRecordsRequest(federatedIdentity,
                "default", "testQueryId", new TableName("default", "test"),
                fieldSchema, splitBuilder.build(), constraints, 1024, 1024);
        evaluator = new CsvFilter().evaluator(recordsRequest);

    }

    @Test
    public void testConstraintEvaluator() throws URISyntaxException
    {
        RowProcessor rowProcessor = evaluator.processor();

        String[] s = {"test", "test"};
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true); // grabs headers from input
        settings.setMaxCharsPerColumn(10240);
        settings.setProcessor(evaluator.processor());
        CsvParser parser = new CsvParser(settings);
        URL resource = getClass().getClassLoader().getResource(CSV_FILE);
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parser.beginParsing(new File(resource.toURI()));
        evaluator.withSpillerAndStatusChecker(mock(BlockSpiller.class), mockedQueryStatusChecker);
        evaluator.recordMetadata(parser.getRecordMetadata());
        rowProcessor.rowProcessed(s, null);
        assertNotNull(rowProcessor);
    }

    @Test
    public void testRecordMetadata() throws URISyntaxException
    {
        ConstraintEvaluator evaluator = spy(CsvConstraintEvaluatorTest.evaluator);
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true); // grabs headers from input
        settings.setMaxCharsPerColumn(10240);
        settings.setProcessor(evaluator.processor());
        CsvParser parser = new CsvParser(settings);
        URL resource = getClass().getClassLoader().getResource(CSV_FILE);
        parser.beginParsing(new File(resource.toURI()));
        evaluator.recordMetadata(parser.getRecordMetadata());
        verify(evaluator, times(1)).recordMetadata(parser.getRecordMetadata());
    }

    @Test
    public void testToString()
    {
        assertNotNull(evaluator.toString());
    }

    @Test
    public void testAddToInExpression()
    {
        CsvConstraintEvaluator evaluator = new CsvConstraintEvaluator();
        evaluator.addToAnd(new And(List.of(new EqualsExpression("test", "test"))));
        evaluator.addToIn(new EqualsExpression("test", "test"));
        evaluator.addToOr(new EqualsExpression("test", "test"));
        assertNotNull(evaluator.in(), "In clauses was null");
        assertFalse(evaluator.in().isEmpty(), "In clauses not found");
    }

}
