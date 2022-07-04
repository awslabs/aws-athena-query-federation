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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Parses Glue type definitions into tokenized form.
 */
public class GlueTypeParser
{
    private static final Logger logger = LoggerFactory.getLogger(GlueTypeParser.class);

    protected static final Character FIELD_START = '<';
    protected static final Character FIELD_END = '>';
    protected static final Character FIELD_DIV = ':';
    protected static final Character FIELD_SEP = ',';
    protected static final Character ESCAPE_CHAR = '\\';
    protected static final Character FIELD_PARAM_START = '(';
    protected static final Character FIELD_PARAM_END = ')';
    private static final Set<Character> TOKENS = new HashSet<>();

    static {
        TOKENS.add(FIELD_START);
        TOKENS.add(FIELD_END);
        TOKENS.add(FIELD_DIV);
        TOKENS.add(FIELD_SEP);
    }

    private final String input;
    private int pos;
    private Token current = null;

    /**
     * Creates a new parser for the provided field type definition as a String.
     *
     * @param input The string representation of the field type definition from Glue.
     */
    public GlueTypeParser(String input)
    {
        this.input = input;
        this.pos = 0;
    }

    /**
     * Does the parser have any more tokens.
     *
     * @return True if the parser may have additional tokens, False otherwise.
     */
    public boolean hasNext()
    {
        return pos < input.length();
    }

    /**
     * Gets the next token.
     *
     * @return The next token or null if no such Token exists.
     */
    public Token next()
    {
        StringBuilder sb = new StringBuilder();
        int readPos = pos;
        int lastEscapeChar = -1;
        // fieldParamsContext is used to ignore tokenization within the field's params
        // For example: "asdfType(3,2,3,4)"
        // Without this we would tokenize at the first comma into: "asdfType(3,"
        // When we really want to just capture the entire type and params: "asdfType(3,2,3,4)"
        boolean fieldParamsContext = false;
        while (input.length() > readPos) {
            Character last = input.charAt(readPos++);
            if (last.equals(ESCAPE_CHAR)) {
                lastEscapeChar = readPos - 1;
            }
            else if (last.equals(' ')) {
                //NoOp
            }
            else if (fieldParamsContext) {
                if (last == FIELD_PARAM_END) {
                    fieldParamsContext = false;
                }
                sb.append(last);
            }
            else if (!TOKENS.contains(last) || lastEscapeChar == readPos - 2) {
                if (last == FIELD_PARAM_START) {
                    fieldParamsContext = true;
                }
                //accumulate if the char is not a TOKEN or is escaped
                sb.append(last);
            }
            else {
                pos = readPos;
                current = new Token(sb.toString(), last, readPos);
                logger.debug("next: {}", current);
                return current;
            }
        }
        pos = readPos;

        current = new Token(sb.toString(), null, readPos);
        logger.debug("next: {}", current);

        return current;
    }

    /**
     * Give the last Token returned by next()
     *
     * @return The Token.
     */
    public Token currentToken()
    {
        return current;
    }

    public static class Token
    {
        private final String value;
        private final Character marker;
        private final int pos;

        public Token(String value, Character marker, int pos)
        {
            this.value = value;
            this.marker = marker;
            this.pos = pos;
        }

        public String getValue()
        {
            return value;
        }

        public Character getMarker()
        {
            return marker;
        }

        public int getPos()
        {
            return pos;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Token token = (Token) o;

            if (getMarker() == null && token.getMarker() != null) {
                return false;
            }

            return getPos() == token.getPos() &&
                    getValue().equals(token.getValue()) &&
                    (getMarker() == null ||
                            getMarker().equals(token.getMarker()));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getValue(), getMarker(), getPos());
        }

        @Override
        public String toString()
        {
            return "Token{" +
                    "value='" + value + '\'' +
                    ", marker=" + marker +
                    ", pos=" + pos +
                    '}';
        }
    }
}
