package com.amazonaws.athena.connector.lambda.metadata.glue;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

/**
 * Extracts field definitions, including complex types like List and STRUCT, from AWS Glue Data Catalog's
 * definition of a field. This class makes use of GlueTypeParser to tokenize the definition of the field.
 * For basic fields, the Lexer's job is straight forward but for complex types it is more involved.
 */
public class GlueFieldLexer
{
    // NOTE:
    // This entire class and file should really be called a parser instead...
    // The GlueFieldParser should actually be called a GlueFieldLexer and vice versa.

    private static final Logger logger = LoggerFactory.getLogger(GlueFieldLexer.class);

    private static final String STRUCT = "struct";

    private static final String MAP = "map";

    private static final Set<String> LIST_EQUIVALENTS = Set.of("array", "set");

    private static final BaseTypeMapper DEFAULT_TYPE_MAPPER = (String type) -> DefaultGlueType.toArrowType(type);

    public static final boolean MAP_DISABLED = true;

    private GlueFieldLexer() {}

    public interface BaseTypeMapper
    {
        //Return Null if the supplied value is not a base type
        ArrowType getType(String type);

        default Field getField(String name, String type)
        {
            if (getType(type) != null) {
                return FieldBuilder.newBuilder(name, getType(type)).build();
            }
            return null;
        }
    }

    public static Field lex(String name, String input)
    {
        return lex(name, input, DEFAULT_TYPE_MAPPER);
    }

    public static Field lex(String name, String input, BaseTypeMapper mapper)
    {
        GlueTypeParser parser = new GlueTypeParser(input);
        return lexInternal(name, parser, mapper);
    }

    private static Field lexInternal(String name, GlueTypeParser parser, BaseTypeMapper mapper)
    {
        logger.debug("lexInternal: enter - {}", name);
        try {
            GlueTypeParser.Token typeToken = parser.next();
            final String typeTokenValueLower = typeToken.getValue().toLowerCase();
            if (typeTokenValueLower.equals(STRUCT)) {
                return parseStruct(name, typeToken, parser, mapper);
            }
            else if (typeTokenValueLower.equals(MAP)) {
                return parseMap(name, typeToken, parser, mapper);
            }
            else if (LIST_EQUIVALENTS.contains(typeTokenValueLower)) {
                return parseList(name, typeToken, parser, mapper);
            }
            // Primitive case
            expectTokenMarkerIsFieldEnd(typeToken);
            return mapper.getField(name, typeTokenValueLower);
        }
        finally {
            logger.debug("lexInternal: exit - {}", name);
        }
    }

    private static Field parseStruct(String name, GlueTypeParser.Token typeToken, GlueTypeParser parser, BaseTypeMapper mapper)
    {
        expectTokenMarkerIsFieldStart(typeToken);
        FieldBuilder fieldBuilder = FieldBuilder.newBuilder(name, Types.MinorType.STRUCT.getType());
        // Iterate through the child element types of the STRUCT
        while (parser.hasNext()) {
            GlueTypeParser.Token childNameToken = parser.next();
            if (!childNameToken.getMarker().equals(GlueTypeParser.FIELD_DIV)) {
                // Current token is not a child if it doesn't have a ":" (FIELD_DIV).
                // Which means we are done with the struct. Just break out and build.
                break;
            }
            // Recursive call to resolve child type
            Field child = lexInternal(childNameToken.getValue(), parser, mapper);
            fieldBuilder.addField(child);
        }
        // Expect that we're ending on the closing token
        expectTokenMarkerIsFieldEnd(parser.currentToken());
        return fieldBuilder.build();
    }

    private static Field parseList(String name, GlueTypeParser.Token typeToken, GlueTypeParser parser, BaseTypeMapper mapper)
    {
        expectTokenMarkerIsFieldStart(typeToken);
        // Recursive call to resolve child element type
        Field child = lexInternal(name, parser, mapper);
        // The next field must always be a closing token since we are building the field here
        // So consume the closing token for this field
        GlueTypeParser.Token closingToken = parser.next();
        // Note that the closing token is , if the enclosing type is a struct
        // and > if the enclosing type is a list
        expectTokenMarkerIsFieldEnd(closingToken);
        return FieldBuilder.newBuilder(name, Types.MinorType.LIST.getType()).addField(child).build();
    }

    private static Field parseMap(String name, GlueTypeParser.Token typeToken, GlueTypeParser parser, BaseTypeMapper mapper)
    {
        if (MAP_DISABLED) {
            throw new RuntimeException("Map type is currently unsupported");
        }

        expectTokenMarkerIsFieldStart(typeToken);
        // Recursive calls to resolve key and value types
        Field keyType = lexInternal("key", parser, mapper);
        Field valueType = lexInternal("value", parser, mapper);
        // The next field must always be a closing token since we are building the field here
        // So consume the closing token for this field
        GlueTypeParser.Token closingToken = parser.next();
        // Note that the closing token is , if the enclosing type is a struct
        // and > if the enclosing type is a list
        expectTokenMarkerIsFieldEnd(closingToken);

        FieldType keyFieldTypeNotNullable = new FieldType(false, keyType.getType(), keyType.getDictionary(), keyType.getMetadata());
        Field keyFieldNotNullable = new Field(keyType.getName(), keyFieldTypeNotNullable, keyType.getChildren());

        return FieldBuilder.newBuilder(name, new ArrowType.Map(false))
             .addField("ENTRIES", Types.MinorType.STRUCT.getType(), false,
                  Arrays.asList(keyFieldNotNullable, valueType))
             .build();
    }

    private static void expectTokenMarkerIsFieldStart(GlueTypeParser.Token token)
    {
        if (token.getMarker() != GlueTypeParser.FIELD_START) {
            throw new RuntimeException("Expected field start: but found " + token.toString());
        }
    }

    private static void expectTokenMarkerIsFieldEnd(GlueTypeParser.Token token)
    {
        boolean isFieldEnd = (token.getMarker() == null ||
              token.getMarker().equals(GlueTypeParser.FIELD_SEP) ||
              token.getMarker().equals(GlueTypeParser.FIELD_END));
        if (!isFieldEnd) {
            throw new RuntimeException("Expected field ending but found: " + token.toString());
        }
    }
}
