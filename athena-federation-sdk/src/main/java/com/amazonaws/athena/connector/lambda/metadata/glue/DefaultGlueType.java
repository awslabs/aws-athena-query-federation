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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Map.entry;

/**
 * Defines the default mapping of AWS Glue Data Catalog types to Apache Arrow types. You can override these by
 * overriding convertField(...) on GlueMetadataHandler.
 */
public class DefaultGlueType
{
    private static final String TIMESTAMPMILLITZ = "timestamptz";
    private static final Set<String> NON_COMPARABALE_SET = Set.of("TIMESTAMPMILLITZ");

    private static final Map<String, ArrowType> TYPE_MAP = Map.ofEntries(
        entry("int", Types.MinorType.INT.getType()),
        entry("varchar", Types.MinorType.VARCHAR.getType()),
        entry("string", Types.MinorType.VARCHAR.getType()),
        entry("bigint", Types.MinorType.BIGINT.getType()),
        entry("double", Types.MinorType.FLOAT8.getType()),
        entry("float", Types.MinorType.FLOAT4.getType()),
        entry("smallint", Types.MinorType.SMALLINT.getType()),
        entry("tinyint", Types.MinorType.TINYINT.getType()),
        entry("boolean", Types.MinorType.BIT.getType()),
        entry("binary", Types.MinorType.VARBINARY.getType()),
        entry("timestamp", Types.MinorType.DATEMILLI.getType()),
        entry("date", Types.MinorType.DATEDAY.getType()),
        // ZoneId.systemDefault().getId() is just a place holder, each row will have a TZ value
        // otherwise fall back to the table configured default TZ
        entry(TIMESTAMPMILLITZ, new ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, ZoneId.systemDefault().getId())));

    // decimal match examples:
    // decimal
    // decimal(1,2)
    // decimal(3,2,1)
    private static final Pattern decimalPattern = Pattern.compile("decimal([(](([0-9]+,?){2,3})[)])?");

    private String id;
    private ArrowType arrowType;

    DefaultGlueType(String id, ArrowType arrowType)
    {
        this.id = id;
        this.arrowType = arrowType;
    }

    private static ArrowType getDecimalArrowType(String in)
    {
        Matcher decimalMatcher = decimalPattern.matcher(in);
        if (!decimalMatcher.matches()) {
            return null;
        }
        try {
            int[] params = Arrays.stream(decimalMatcher.group(2).split(","))
                    .mapToInt(Integer::parseInt).toArray();
            if (params.length == 2) {
                return new ArrowType.Decimal(params[0], params[1]);
            }
            // else this must be 3 because of the regex
            return new ArrowType.Decimal(params[0], params[1], params[2]);
        }
        catch (java.lang.NullPointerException e) {
            // This is the case where it is only "decimal" with no parameters
            // Using the default precision and scale that spark sql defaults
            // to when no parameters are specified.
            // NOTE: I would prefer to check the decimalMatcher.groupCount() above
            // rather than a try catch but decimalMatcher.groupCount() always returns
            // 3 for some reason...
            return new ArrowType.Decimal(38, 18);
        }
    }

    public static ArrowType fromId(String id)
    {
        ArrowType result = toArrowType(id);
        if (result == null) {
            throw new IllegalArgumentException("Unknown DefaultGlueType for id: " + id);
        }
        return result;
    }

    public static ArrowType toArrowType(String id)
    {
        ArrowType result = TYPE_MAP.get(id.toLowerCase());
        if (result == null) {
            return getDecimalArrowType(id);
        }

        return result;
    }

    public ArrowType getArrowType()
    {
        return arrowType;
    }

    public static Set<String> getNonComparableSet()
    {
        return NON_COMPARABALE_SET;
    }
}
