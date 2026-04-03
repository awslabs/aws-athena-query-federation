/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * This class is used to test the ElasticsearchSchemaUtils class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchSchemaUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSchemaUtilsTest.class);

    Schema expectedSchema;
    LinkedHashMap<String, Object> mapping;

    @Before
    public void setUp()
            throws IOException
    {
        expectedSchema = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .addField("mykeyword", Types.MinorType.VARCHAR.getType())
                .addField(new Field("mylong", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mylong",
                                FieldType.nullable(Types.MinorType.BIGINT.getType()), null))))
                .addField("myinteger", Types.MinorType.INT.getType())
                .addField("myshort", Types.MinorType.SMALLINT.getType())
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .addField(new Field("myscaled",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.0")), null))
                .addField("myfloat", Types.MinorType.FLOAT8.getType())
                .addField("myhalf", Types.MinorType.FLOAT8.getType())
                .addField("mydatemilli", Types.MinorType.DATEMILLI.getType())
                .addField("mydatenano", Types.MinorType.DATEMILLI.getType())
                .addField("myboolean", Types.MinorType.BIT.getType())
                .addField("mybinary", Types.MinorType.VARCHAR.getType())
                .addField("mynested", Types.MinorType.STRUCT.getType(), ImmutableList.of(
                        new Field("l1long", FieldType.nullable(Types.MinorType.BIGINT.getType()), null),
                        new Field("l1date", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null),
                        new Field("l1nested", FieldType.nullable(Types.MinorType.STRUCT.getType()), ImmutableList.of(
                                new Field("l2short", FieldType.nullable(Types.MinorType.LIST.getType()),
                                        Collections.singletonList(new Field("l2short",
                                                FieldType.nullable(Types.MinorType.SMALLINT.getType()), null))),
                                new Field("l2binary", FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                                        null))))).build();

        mapping = new ObjectMapper().readValue(
                "{\n" +
                        "\"_meta\" : {\n" +                               // _meta:
                        "  \"mynested.l1nested.l2short\" : \"list\",\n" + // mynested.l1nested.l2short: LIST<SMALLINT>
                        "  \"mylong\" : \"list\"\n" +                     // mylong: LIST<BIGINT>
                        "},\n" +
                        "\"properties\" : {\n" +
                        "  \"mybinary\" : {\n" +                          // mybinary:
                        "    \"type\" : \"binary\"\n" +                   // type: binary (VARCHAR)
                        "  },\n" +
                        "  \"myboolean\" : {\n" +                         // myboolean:
                        "    \"type\" : \"boolean\"\n" +                  // type: boolean (BIT)
                        "  },\n" +
                        "  \"mybyte\" : {\n" +                            // mybyte:
                        "    \"type\" : \"byte\"\n" +                     // type: byte (TINYINT)
                        "  },\n" +
                        "  \"mydatemilli\" : {\n" +                       // mydatemilli:
                        "    \"type\" : \"date\"\n" +                     // type: date (DATEMILLI)
                        "  },\n" +
                        "  \"mydatenano\" : {\n" +                        // mydatenano:
                        "    \"type\" : \"date_nanos\"\n" +               // type: date_nanos (DATEMILLI)
                        "  },\n" +
                        "  \"mydouble\" : {\n" +                          // mydouble:
                        "    \"type\" : \"double\"\n" +                   // type: double (FLOAT8)
                        "  },\n" +
                        "  \"myfloat\" : {\n" +                           // myfloat:
                        "    \"type\" : \"float\"\n" +                    // type: float (FLOAT8)
                        "  },\n" +
                        "  \"myhalf\" : {\n" +                            // myhalf:
                        "    \"type\" : \"half_float\"\n" +               // type: half_float (FLOAT8)
                        "  },\n" +
                        "  \"myinteger\" : {\n" +                         // myinteger:
                        "    \"type\" : \"integer\"\n" +                  // type: integer (INT)
                        "  },\n" +
                        "  \"mykeyword\" : {\n" +                         // mykeyword:
                        "    \"type\" : \"keyword\"\n" +                  // type: keyword (VARCHAR)
                        "    },\n" +
                        "    \"mylong\" : {\n" +                            // mylong: LIST
                        "      \"type\" : \"long\"\n" +                     // type: long (BIGINT)
                        "    },\n" +
                        "    \"mynested\" : {\n" +                          // mynested: STRUCT
                        "      \"properties\" : {\n" +
                        "        \"l1date\" : {\n" +                        // mynested.l1date:
                        "          \"type\" : \"date_nanos\"\n" +           // type: date_nanos (DATEMILLI)
                        "        },\n" +
                        "        \"l1long\" : {\n" +                        // mynested.l1long:
                        "          \"type\" : \"long\"\n" +                 // type: long (BIGINT)
                        "        },\n" +
                        "        \"l1nested\" : {\n" +                      // mynested.l1nested: STRUCT
                        "          \"properties\" : {\n" +
                        "            \"l2binary\" : {\n" +                  // mynested.l1nested.l2binary:
                        "              \"type\" : \"binary\"\n" +           // type: binary (VARCHAR)
                        "            },\n" +
                        "            \"l2short\" : {\n" +                   // mynested.l1nested.l2short: LIST
                        "              \"type\" : \"short\"\n" +            // type: short (SMALLINT)
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"myscaled\" : {\n" +                          // myscaled:
                        "      \"type\" : \"scaled_float\",\n" +            // type: scaled_float (BIGINT)
                        "      \"scaling_factor\" : 10.0\n" +               // factor: 10
                        "    },\n" +
                        "    \"myshort\" : {\n" +                           // myshort:
                        "      \"type\" : \"short\"\n" +                    // type: short (SMALLINT)
                        "    },\n" +
                        "    \"mytext\" : {\n" +                            // mytext:
                        "      \"type\" : \"text\"\n" +                     // type: text (VARCHAR)
                        "    }\n" +
                        "  }\n" +
                        "}\n", LinkedHashMap.class);
    }

    @Test
    public void parseMapping_withValidMapping_returnsSchema()
    {
        logger.info("parseMapping_withValidMapping_returnsSchema - enter");

        Schema builtSchema = ElasticsearchSchemaUtils.parseMapping(mapping);

        // The built mapping and expected mapping should match.
        assertTrue("Real and mocked mappings are different!",
                ElasticsearchSchemaUtils.mappingsEqual(expectedSchema, builtSchema));

        logger.info("parseMapping_withValidMapping_returnsSchema - exit");
    }

    @Test
    public void parseMapping_withListOfStruct_returnsSchema()
            throws JsonProcessingException
    {

        Schema expected = SchemaBuilder.newBuilder()
                .addField("director", Types.MinorType.VARCHAR.getType())
                .addField("objlistouter", Types.MinorType.LIST.getType(),
                        ImmutableList.of(
                                new Field("objlistouter", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                                        ImmutableList.of(
                                                new Field("objlistinner", FieldType.nullable(Types.MinorType.LIST.getType()),
                                                        ImmutableList.of(new Field("objlistinner", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                                                                ImmutableList.of(
                                                                        new Field("title", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
                                                                        new Field("hi", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))),
                                                new Field("test2", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))
                                )
                        ))
                .addField("title", Types.MinorType.VARCHAR.getType())
                .addField("year", Types.MinorType.BIGINT.getType())
                .build();

        LinkedHashMap actual = new ObjectMapper().readValue(" {\n" +
                "      \"_meta\" : {\n" +
                "        \"objlistouter\" : \"list\",\n" +
                "        \"objlistouter.objlistinner\" : \"list\"\n" +
                "      },\n" +
                "      \"properties\" : {\n" +
                "        \"director\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"fields\" : {\n" +
                "            \"keyword\" : {\n" +
                "              \"type\" : \"keyword\",\n" +
                "              \"ignore_above\" : 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"objlistouter\" : {\n" +
                "          \"properties\" : {\n" +
                "            \"objlistinner\" : {\n" +
                "              \"properties\" : {\n" +
                "                \"hi\" : {\n" +
                "                  \"type\" : \"text\",\n" +
                "                  \"fields\" : {\n" +
                "                    \"keyword\" : {\n" +
                "                      \"type\" : \"keyword\",\n" +
                "                      \"ignore_above\" : 256\n" +
                "                    }\n" +
                "                  }\n" +
                "                },\n" +
                "                \"title\" : {\n" +
                "                  \"type\" : \"text\",\n" +
                "                  \"fields\" : {\n" +
                "                    \"keyword\" : {\n" +
                "                      \"type\" : \"keyword\",\n" +
                "                      \"ignore_above\" : 256\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            },\n" +
                "            \"test2\" : {\n" +
                "              \"type\" : \"text\",\n" +
                "              \"fields\" : {\n" +
                "                \"keyword\" : {\n" +
                "                  \"type\" : \"keyword\",\n" +
                "                  \"ignore_above\" : 256\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"title\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"fields\" : {\n" +
                "            \"keyword\" : {\n" +
                "              \"type\" : \"keyword\",\n" +
                "              \"ignore_above\" : 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"year\" : {\n" +
                "          \"type\" : \"long\"\n" +
                "        }\n" +
                "      }\n" +
                "    }", LinkedHashMap.class);

        logger.info("parseMapping_withListOfStruct_returnsSchema - enter");

        Schema builtSchema = ElasticsearchSchemaUtils.parseMapping(actual);
        // The built mapping and expected mapping should match.
        assertTrue("Real and mocked mappings are different!",
                ElasticsearchSchemaUtils.mappingsEqual(expected, builtSchema));

        logger.info("parseMapping_withListOfStruct_returnsSchema - exit");
    }

    @Test
    public void parseMapping_withInvalidMeta_throwsAthenaConnectorException()
            throws JsonProcessingException
    {
        LinkedHashMap actual = new ObjectMapper().readValue(" {\n" +
                "      \"_meta\" : {\n" +
                "        \"objlistouter\" : \"hi\",\n" +
                "        \"objlistouter.objlistinner\" : \"test\"\n" +
                "      },\n" +
                "      \"properties\" : {\n" +
                "        \"director\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"fields\" : {\n" +
                "            \"keyword\" : {\n" +
                "              \"type\" : \"keyword\",\n" +
                "              \"ignore_above\" : 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"objlistouter\" : {\n" +
                "          \"properties\" : {\n" +
                "            \"objlistinner\" : {\n" +
                "              \"properties\" : {\n" +
                "                \"hi\" : {\n" +
                "                  \"type\" : \"text\",\n" +
                "                  \"fields\" : {\n" +
                "                    \"keyword\" : {\n" +
                "                      \"type\" : \"keyword\",\n" +
                "                      \"ignore_above\" : 256\n" +
                "                    }\n" +
                "                  }\n" +
                "                },\n" +
                "                \"title\" : {\n" +
                "                  \"type\" : \"text\",\n" +
                "                  \"fields\" : {\n" +
                "                    \"keyword\" : {\n" +
                "                      \"type\" : \"keyword\",\n" +
                "                      \"ignore_above\" : 256\n" +
                "                    }\n" +
                "                  }\n" +
                "                }\n" +
                "              }\n" +
                "            },\n" +
                "            \"test2\" : {\n" +
                "              \"type\" : \"text\",\n" +
                "              \"fields\" : {\n" +
                "                \"keyword\" : {\n" +
                "                  \"type\" : \"keyword\",\n" +
                "                  \"ignore_above\" : 256\n" +
                "                }\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"title\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"fields\" : {\n" +
                "            \"keyword\" : {\n" +
                "              \"type\" : \"keyword\",\n" +
                "              \"ignore_above\" : 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"year\" : {\n" +
                "          \"type\" : \"long\"\n" +
                "        }\n" +
                "      }\n" +
                "    }", LinkedHashMap.class);

        logger.info("parseMapping_withInvalidMeta_throwsAthenaConnectorException - enter");

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                () -> ElasticsearchSchemaUtils.parseMapping(actual));
        assertTrue("Exception message should contain _meta only support value",
                ex.getMessage().contains("_meta only support value"));

        logger.info("parseMapping_withInvalidMeta_throwsAthenaConnectorException - exit");
    }

    @Test
    public void parseMapping_withUnknownType_returnsFieldWithNullType()
    {
        Map<String, Object> field1 = new LinkedHashMap<>();
        field1.put("type", "unknown_type");
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("field1", field1);
        Map<String, Object> mapping = new LinkedHashMap<>();
        mapping.put("properties", properties);

        Schema schema = ElasticsearchSchemaUtils.parseMapping(mapping);
        Field field = schema.findField("field1");
        
        assertNotNull("Schema should contain field1", field);
        assertSame("Should return NULL type for unknown type", Types.MinorType.NULL, Types.getMinorTypeForArrowType(field.getType()));
    }

    @Test
    public void mappingsEqual_withDifferentSizes_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField("field1", Types.MinorType.VARCHAR.getType())
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField("field1", Types.MinorType.VARCHAR.getType())
                .addField("field2", Types.MinorType.INT.getType())
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when schema sizes differ", result);
    }

    @Test
    public void mappingsEqual_withDifferentFieldTypes_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField("field1", Types.MinorType.VARCHAR.getType())
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField("field1", Types.MinorType.INT.getType())
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when field types differ", result);
    }

    @Test
    public void mappingsEqual_withDifferentMetadata_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                        ImmutableMap.of("scaling_factor", "100")), null))
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                        ImmutableMap.of("scaling_factor", "200")), null))
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when metadata differs", result);
    }

    @Test
    public void mappingsEqual_withDifferentChildSizes_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child1", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(
                                new Field("child1", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
                                new Field("child2", FieldType.nullable(Types.MinorType.INT.getType()), null))))
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when child sizes differ", result);
    }

    @Test
    public void mappingsEqual_withDifferentChildNames_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child1", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child2", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when child names differ", result);
    }

    @Test
    public void mappingsEqual_withDifferentChildFieldTypes_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child1", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null))))
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child1", FieldType.nullable(Types.MinorType.INT.getType()), null))))
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when child field types differ", result);
    }

    @Test
    public void mappingsEqual_withDifferentChildMetadata_returnsFalse()
    {
        Schema schema1 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child1", new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "100")), null))))
                .build();
        Schema schema2 = SchemaBuilder.newBuilder()
                .addField(new Field("field1", FieldType.nullable(Types.MinorType.STRUCT.getType()),
                        ImmutableList.of(new Field("child1", new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "200")), null))))
                .build();

        boolean result = ElasticsearchSchemaUtils.mappingsEqual(schema1, schema2);
        assertFalse("mappingsEqual should return false when child metadata differs", result);
    }
}
