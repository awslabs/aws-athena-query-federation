/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.TestBase;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.COLLECTION;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.QUERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RDFHandlerTest extends TestBase {
    private static final String VERTEX_TYPE = "vertex";
    private static final String SPARQL_TYPE = "sparql";
    private static final String CLASS_TYPE = "class";
    private static final String INVALID_TYPE = "invalid";
    private static final String QUERY_MODE_CLASS = "class";
    private static final String INVALID_QUERY_MODE = "invalid";
    private static final String CLASS_URI = "<http://example.org/Person>";
    private static final String PREDS_PREFIX = "ex";
    private static final String SUBJECT_PERSON = "person";
    private static final String PERSON_FIELD = "person";
    private static final String NAME_FIELD = "name";
    private static final String AGE_FIELD = "age";
    private static final String S_FIELD = "s";
    private static final String P_FIELD = "p";
    private static final String O_FIELD = "o";
    private static final String SCHEMA_FUNCTION = "system.query";
    private static final String SYSTEM_QUERY = "SYSTEM.QUERY";
    private static final String DEFAULT_DATABASE = "default";
    private static final String TRIPLES_COLLECTION = "triples";
    private static final String SPARQL_QUERY = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    private static final String PERSON_URI = "http://example.org/person1";
    private static final String JOHN_DOE = "John Doe";
    private static final String AGE_30 = "30";
    private static final String SUBJECT_URI = "http://example.org/subject";
    private static final String PREDICATE_URI = "http://example.org/predicate";
    private static final String OBJECT_URI = "http://example.org/object";
    private static final String EX_PREFIX = "http://example.org/";
    private static final String SPARQL_METADATA = "sparql";
    
    private RDFHandler handler;
    @Mock
    private NeptuneSparqlConnection sparqlConnection;
    @Mock
    private BlockSpiller spiller;
    @Mock
    private QueryStatusChecker checker;

    @Before
    public void setUp() throws Exception {
        doNothing().when(sparqlConnection).runQuery(anyString());
        handler = new RDFHandler(sparqlConnection);
        checker = mock(QueryStatusChecker.class);
        when(checker.isQueryRunning()).thenReturn(true);
        spiller = mock(BlockSpiller.class);
        doAnswer(invocation -> null).when(spiller).writeRows(any());
    }

    @Test
    public void executeQuery_WithSparqlMode_ProcessesQuery() throws Exception {
        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createReadRecordsRequest(schema, Collections.emptyMap());
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        Map<String, Object> result = createTestResult();
        when(sparqlConnection.next()).thenReturn(result);

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());

        verify(sparqlConnection).runQuery(argThat(query ->
                 query.contains("SELECT ?s ?p ?o WHERE")));
        verify(spiller, times(1)).writeRows(any());
    }

    @Test(expected = RuntimeException.class)
    public void executeQuery_WithRDFReadErrors_ThrowsRuntimeException() throws Exception {
        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createReadRecordsRequest(schema, Collections.emptyMap());
        doThrow(new RuntimeException()).when(sparqlConnection).runQuery(anyString());

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test(expected = RuntimeException.class)
    public void executeQuery_WithInvalidGraphType_ThrowsRuntimeException() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
            .addMetadata("componenttype", VERTEX_TYPE)
            .addStringField("id")
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema, Collections.emptyMap());
        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test
    public void executeQuery_WithClassMode_ProcessesQuery() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_QUERY_MODE, Constants.QUERY_MODE_CLASS)
                .addMetadata(Constants.SCHEMA_CLASS_URI, CLASS_URI)
                .addMetadata(Constants.SCHEMA_PREDS_PREFIX, PREDS_PREFIX)
                .addMetadata(Constants.SCHEMA_SUBJECT, SUBJECT_PERSON)
                .addStringField(PERSON_FIELD)
                .addStringField(NAME_FIELD)
                .addStringField(AGE_FIELD)
                .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema, Collections.emptyMap());
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        Map<String, Object> result = new HashMap<>();
        result.put(PERSON_FIELD, PERSON_URI);
        result.put(NAME_FIELD, JOHN_DOE);
        result.put(AGE_FIELD, AGE_30);
        when(sparqlConnection.next()).thenReturn(result);
        when(checker.isQueryRunning()).thenReturn(true);

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());

        verify(sparqlConnection).runQuery(anyString());
        verify(spiller, times(1)).writeRows(any());
    }

    @Test(expected = RuntimeException.class)
    public void executeQuery_WithInvalidQueryMode_ThrowsRuntimeException() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_QUERY_MODE, INVALID_QUERY_MODE)
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, INVALID_TYPE)
                .addStringField(S_FIELD)
                .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema, Collections.emptyMap());
        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test(expected = RuntimeException.class)
    public void executeQuery_WithMissingClassUri_ThrowsRuntimeException() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
            .addMetadata(Constants.SCHEMA_QUERY_MODE, QUERY_MODE_CLASS)
            .addMetadata(Constants.SCHEMA_PREDS_PREFIX, PREDS_PREFIX)
            .addMetadata(Constants.SCHEMA_SUBJECT, SUBJECT_PERSON)
            .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, CLASS_TYPE)
            .addStringField(PERSON_FIELD)
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema, Collections.emptyMap());
        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test
    public void executeQuery_WithRDFQueryPassthrough_ProcessesPassthroughQuery() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_QUERY_MODE, Constants.QUERY_MODE_SPARQL)
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, SPARQL_TYPE)
                .addMetadata("schema.function", SCHEMA_FUNCTION)
                .addStringField(S_FIELD)
                .addStringField(P_FIELD)
                .addStringField(O_FIELD)
                .build();

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("schemaFunctionName", SYSTEM_QUERY);
        passthroughArgs.put(DATABASE, DEFAULT_DATABASE);
        passthroughArgs.put(COLLECTION, TRIPLES_COLLECTION);
        passthroughArgs.put(QUERY, SPARQL_QUERY);

        ReadRecordsRequest request = createReadRecordsRequest(schema, passthroughArgs);
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        Map<String, Object> result = createTestResult();
        when(sparqlConnection.next()).thenReturn(result);

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());

        verify(sparqlConnection).runQuery(anyString());
        verify(spiller, times(1)).writeRows(any());
    }

    private Map<String, Object> createTestResult() {
        Map<String, Object> result = new HashMap<>();
        result.put(S_FIELD, SUBJECT_URI);
        result.put(P_FIELD, PREDICATE_URI);
        result.put(O_FIELD, OBJECT_URI);
        return result;
    }

    private ReadRecordsRequest createReadRecordsRequest(Schema schema, Map<String, String> passthroughArgs) {
        S3SpillLocation spillLoc = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(UUID.randomUUID().toString())
            .withIsDirectory(true)
            .build();

        return new ReadRecordsRequest(
            IDENTITY,
            DEFAULT_CATALOG,
            QUERY_ID,
            TABLE_NAME,
            schema,
            Split.newBuilder(spillLoc, new LocalKeyFactory().create()).build(),
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, passthroughArgs, null),
            1_500_000L,
            0L
        );
    }

    private Schema createRDFSchema() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(Constants.SCHEMA_QUERY_MODE, Constants.QUERY_MODE_SPARQL);
        metadata.put(Constants.SCHEMA_COMPONENT_TYPE, SPARQL_TYPE);
        metadata.put(Constants.PREFIX_KEY + PREDS_PREFIX, EX_PREFIX);
        metadata.put(SPARQL_METADATA, SPARQL_QUERY);

        List<Field> fields = Arrays.asList(
            new Field(S_FIELD, FieldType.nullable(new ArrowType.Utf8()), null),
            new Field(P_FIELD, FieldType.nullable(new ArrowType.Utf8()), null),
            new Field(O_FIELD, FieldType.nullable(new ArrowType.Utf8()), null)
        );
        return new Schema(fields, metadata);
    }
} 