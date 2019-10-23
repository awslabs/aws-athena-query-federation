package com.amazonaws.athena.connector.lambda.metadata.glue;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlueFieldLexer
{
    private static final Logger logger = LoggerFactory.getLogger(GlueFieldLexer.class);

    private static String STRUCT = "struct";
    private static String LIST = "array";

    private static BaseTypeMapper DEFAULT_TYPE_MAPPER = (String type) -> DefaultGlueType.toArrowType(type);

    private GlueFieldLexer() {}

    public interface BaseTypeMapper
    {
        //Return Null if the supplied value is not a base type
        ArrowType getType(String type);
    }

    public static Field lex(String name, String input)
    {
        if (DEFAULT_TYPE_MAPPER.getType(input) != null) {
            return FieldBuilder.newBuilder(name, DEFAULT_TYPE_MAPPER.getType(input)).build();
        }

        GlueTypeParser parser = new GlueTypeParser(input);
        return lexComplex(name, parser.next(), parser, DEFAULT_TYPE_MAPPER);
    }

    public static Field lex(String name, String input, BaseTypeMapper mapper)
    {
        if (mapper.getType(input) != null) {
            return FieldBuilder.newBuilder(name, mapper.getType(input)).build();
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
            return FieldBuilder.newBuilder(name, Types.MinorType.LIST.getType())
                    .addField(FieldBuilder.newBuilder(name, mapper.getType(arrayType.getValue())).build())
                    .build();
        }
        else {
            throw new RuntimeException("Unexpected start type " + startToken.getValue());
        }

        while (parser.hasNext() && parser.currentToken().getMarker() != GlueTypeParser.FIELD_END) {
            Field child = lex(parser.next(), parser, mapper);
            fieldBuilder.addField(child);
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
            return FieldBuilder.newBuilder(name, mapper.getType(typeToken.getValue())).build();
        }
        throw new RuntimeException("Unexpected Token " + typeToken.getValue() + "[" + typeToken.getMarker() + "]"
                + " @ " + typeToken.getPos() + " while processing " + name);
    }
}
