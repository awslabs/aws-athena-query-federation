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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts field definitions, including complex types like List and STRUCT, from AWS Glue Data Catalog's
 * definition of a field. This class makes use of GlueTypeParser to tokenize the definition of the field.
 * For basic fields, the Lexer's job is straight forward but for complex types it is more involved.
 */
public class GlueFieldLexer
{
    private static final Logger logger = LoggerFactory.getLogger(GlueFieldLexer.class);

    private static final String STRUCT = "struct";
    private static final String LIST = "array";

    private static final BaseTypeMapper DEFAULT_TYPE_MAPPER = (String type) -> DefaultGlueType.toArrowType(type);

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
        ArrowType typeResult = DEFAULT_TYPE_MAPPER.getType(input);
        if (typeResult != null) {
            return FieldBuilder.newBuilder(name, typeResult).build();
        }

        GlueTypeParser parser = new GlueTypeParser(input);
        return lexComplex(name, parser.next(), parser, DEFAULT_TYPE_MAPPER);
    }

    public static Field lex(String name, String input, BaseTypeMapper mapper)
    {
        Field result = mapper.getField(name, input);
        if (result != null) {
            return result;
        }

        GlueTypeParser parser = new GlueTypeParser(input);
        return lexComplex(name, parser.next(), parser, mapper);
    }

    private static Field lexComplex(String name, GlueTypeParser.Token startToken, GlueTypeParser parser, BaseTypeMapper mapper)
    {
        FieldBuilder fieldBuilder;

        logger.debug("lexComplex: enter - {}", name);
        if (startToken.getMarker() != GlueTypeParser.FIELD_START) {
            throw new RuntimeException("Parse error, expected " + GlueTypeParser.FIELD_START
                    + " but found " + startToken.getMarker());
        }

        if (startToken.getValue().toLowerCase().equals(STRUCT)) {
            fieldBuilder = FieldBuilder.newBuilder(name, Types.MinorType.STRUCT.getType());
        }
        else if (startToken.getValue().toLowerCase().equals(LIST)) {
            GlueTypeParser.Token arrayType = parser.next();
            Field child;
            String type = arrayType.getValue().toLowerCase();
            if (type.equals(STRUCT) || type.equals(LIST)) {
                child = lexComplex(name, arrayType, parser, mapper);
            }
            else {
                child = mapper.getField(name, arrayType.getValue());
            }
            return FieldBuilder.newBuilder(name, Types.MinorType.LIST.getType()).addField(child).build();
        }
        else {
            throw new RuntimeException("Unexpected start type " + startToken.getValue());
        }

        while (parser.hasNext() && parser.currentToken().getMarker() != GlueTypeParser.FIELD_END) {
            Field child = lex(parser.next(), parser, mapper);
            fieldBuilder.addField(child);
            if (Types.getMinorTypeForArrowType(child.getType()) == Types.MinorType.LIST) {
                // An ARRAY Glue type (LIST in Arrow) within a STRUCT has the same ending token as a STRUCT (">" or
                // GlueTypeParser.FIELD_END). If allowed to proceed, the Glue parser will misinterpret the end of the
                // ARRAY to be the end of the STRUCT (which is currently being processed) ending the loop prematurely
                // and causing all subsequent fields in the STRUCT to be dropped.
                // Example: movies: STRUCT<actors:ARRAY<STRING>,genre:ARRAY<STRING>>
                // will result in Field definition: movies: Struct<actors: List<actors: Utf8>>.
                // In order to prevent that from happening, we must consume an additional token to get past the LIST's
                // ending token ">".
                parser.next();
            }
        }
        parser.next();

        logger.debug("lexComplex: exit - {}", name);
        return fieldBuilder.build();
    }

    private static Field lex(GlueTypeParser.Token startToken, GlueTypeParser parser, BaseTypeMapper mapper)
    {
        GlueTypeParser.Token nameToken = startToken;
        logger.debug("lex: enter - {}", nameToken.getValue());
        if (!nameToken.getMarker().equals(GlueTypeParser.FIELD_DIV)) {
            throw new RuntimeException("Expected Field DIV but found " + nameToken.getMarker() +
                    " while processing " + nameToken.getValue());
        }

        String name = nameToken.getValue();

        GlueTypeParser.Token typeToken = parser.next();
        if (typeToken.getMarker().equals(GlueTypeParser.FIELD_START)) {
            logger.debug("lex: exit - {}", nameToken.getValue());
            return lexComplex(name, typeToken, parser, mapper);
        }
        else if (typeToken.getMarker().equals(GlueTypeParser.FIELD_SEP) ||
                typeToken.getMarker().equals(GlueTypeParser.FIELD_END)
        ) {
            logger.debug("lex: exit - {}", nameToken.getValue());
            return mapper.getField(name, typeToken.getValue());
        }
        throw new RuntimeException("Unexpected Token " + typeToken.getValue() + "[" + typeToken.getMarker() + "]"
                + " @ " + typeToken.getPos() + " while processing " + name);
    }
}
