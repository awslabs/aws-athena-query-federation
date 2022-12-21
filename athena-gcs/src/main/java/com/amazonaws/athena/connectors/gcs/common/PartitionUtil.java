/*-
 * #%L
 * athena-storage-api
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
package com.amazonaws.athena.connectors.gcs.common;

import com.amazonaws.athena.connectors.gcs.UncheckedGcsConnectorException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;
import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.getFolderName;
import static java.util.Objects.requireNonNull;

public class PartitionUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUtil.class);

    private static final Pattern PARTITION_PATTERN = Pattern.compile("(.*)(\\{.*?})(.*?)");

    private static final String DEFAULT_DATE_REGEX_STRING = "\\d{4}-\\d{2}-\\d{2}";

    private PartitionUtil()
    {
    }

    public static String getRootName(String prefix)
    {
        int indexOfPathSeparator = prefix.indexOf("/");
        if (indexOfPathSeparator > -1) {
            return prefix.substring(0, indexOfPathSeparator);
        }
        return prefix;
    }

    public static boolean isPartitionFolder(String folderName)
    {
        String simpleName = getFolderName(folderName);
        boolean matched = FieldValuePatternMatcher.matches(simpleName);
        LOGGER.debug("Folder {} matches with the pattern of a partition table {}", folderName, matched);
        return matched;
    }

    public static Optional<FieldValue> getPartitionFieldValue(String folderName)
    {
        String simpleName = getFolderName(folderName);
        LOGGER.debug("Creating FieldValue from partition folder {}" + simpleName);
        return FieldValue.from(simpleName);
    }

    private static class FieldValuePatternMatcher
    {
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_SINGLE_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)='(.*?)(?<!\\\\)'");
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_DOUBLE_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)=\"(.*?)(?<!\\\\)\"");
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_DOUBLE_NO_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)=(.*?)(?<!\\\\)");

        public static boolean matches(String maybeFieldValue)
        {
            return matchesSingleQuoted(maybeFieldValue)
                    || matchesDoubleQuoted(maybeFieldValue)
                    || matchesUnQuoted(maybeFieldValue);
        }

        private static boolean matchesSingleQuoted(String maybeFieldValue)
        {
            return FIELD_EQUAL_VALUE_PATTERN_SINGLE_QUOTED_PATTERN.matcher(maybeFieldValue).matches();
        }

        private static boolean matchesDoubleQuoted(String maybeFieldValue)
        {
            return FIELD_EQUAL_VALUE_PATTERN_DOUBLE_QUOTED_PATTERN.matcher(maybeFieldValue).matches();
        }

        private static boolean matchesUnQuoted(String maybeFieldValue)
        {
            return FIELD_EQUAL_VALUE_PATTERN_DOUBLE_NO_QUOTED_PATTERN.matcher(maybeFieldValue).matches();
        }
    }

    public static Optional<String> getRegExExpression(Table table)
    {
        Map<String, String> tableParameters = table.getParameters();
        List<Column> partitionColumns = table.getPartitionKeys();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_PATTERN);
        if (partitionPattern == null || partitionPattern.isBlank()) {
            return Optional.empty();
        }
        Matcher partitinoMatcher = PARTITION_PATTERN.matcher(partitionPattern);
        String folderMatchingPattern = "";
        if (partitinoMatcher.matches()) {
            String[] folderParts = partitionPattern.split("/");
            for (String folderPart : folderParts) {
                // TODO:
                // 1. check this part contains any quantifier
                // 2. If above check fails, check whether any of '{' or '}' character presents
                // 3. If above checks pass, throw error, proceed otherwise
                folderMatchingPattern += getFolderValuePattern(partitionColumns, folderPart, tableParameters) + "/";
            }
        }

        if (!folderMatchingPattern.isBlank() && !folderMatchingPattern.endsWith("/")) {
            folderMatchingPattern += "/";
        }

        if (!folderMatchingPattern.isBlank()) {
            if (folderMatchingPattern.contains("{") || folderMatchingPattern.contains("}")) { // TODO: this check needs to be done in each part of the folder above
                throw new UncheckedGcsConnectorException("partition.partition parameter is either invalid or contains a column variable " +
                        "which is not the part of partitions. Pattern is: " + partitionPattern);
            }
            return Optional.of(folderMatchingPattern);
        }
        return Optional.empty();
    }

    public static List<ColumnPrefix> getColumnPrefixes(String folderModel, String folderNameRegEx, List<Column> partitionColumns)
    {
        List<ColumnPrefix> columnPrefixes = new ArrayList<>();
        String[] regExParts = folderNameRegEx.split("/");
        String[] folderParts = folderModel.split("/");
        if (folderParts.length >= regExParts.length) {
            for (int i = 0; i < regExParts.length; i++) {
                Matcher matcher = Pattern.compile(regExParts[i]).matcher(folderParts[i]);
                if (matcher.matches() && matcher.groupCount() > 1) {
                    String regExPrefix = matcher.group(2);
                    Optional<String> optionalColName = getMatchingColumnFromPrefix(partitionColumns, regExPrefix);
                    optionalColName.ifPresent(s -> columnPrefixes.add(new ColumnPrefix(s, regExPrefix)));
                }
            }
        }
        return columnPrefixes;
    }

    public static Optional<PartitionFolder> getPartitionFolder(String folderName, String folderNameRegEx, List<ColumnPrefix> columnPrefixes)
    {
        String[] folderParts = folderName.split("/");
        for (String folder : folderParts) {

        }
        return Optional.empty();
    }



    // helpers
    private static Optional<String> getMatchingColumnFromPrefix(List<Column> columns, String prefix)
    {
        if (prefix != null && !prefix.isBlank() && !columns.isEmpty()) {
            for(Column column : columns) {
                if (prefix.contains(column.getName())) {
                    return Optional.of(column.getName());
                }
            }
        }
        return Optional.empty();
    }

    private static String getFolderValuePattern(List<Column> partitionColumns, String folderPart, Map<String, String> tableParameters)
    {
        Matcher partitinoMatcher = PARTITION_PATTERN.matcher(folderPart);
        if (partitinoMatcher.matches() && partitinoMatcher.groupCount() > 1) {
            String variable = partitinoMatcher.group(2);
            String columnName = variable.replaceAll("[{}]", "");
            for (Column column : partitionColumns) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    String regEx = requireNonNull(getRegExByColumnType(column.getName(), column.getType(), tableParameters));
                    String regExWithPattern = createGroup(folderPart.replace(variable, regEx), regEx);
                    System.out.println(regExWithPattern);
                    return regExWithPattern;
                }
            }
        }
        return folderPart;
    }

    private static String createGroup(String folderPattern, String variableRegEx)
    {
        int index = folderPattern.lastIndexOf(variableRegEx);
        if (index > 1) {
            return "(" + folderPattern.substring(0, index) + ")" + folderPattern.substring(index);
        }
        return folderPattern;
    }

    private static String getRegExByColumnType(String columnName, String columnType, Map<String, String> tableParameters)
    {
        switch (columnType) {
            case "string":
            case "varchar":
                return "(.*?)";
            case "bigint":
            case "int":
            case "smallint":
            case "tinyint":
//                return "\\d+";
                return "(\\d+)";
            case "date":
                String datePattern = tableParameters.get(String.format("partition.%s.pattern", columnName));
                if (datePattern == null) {
                    return "(" + DEFAULT_DATE_REGEX_STRING + ")";
                }
                return "(" + getDateRegExByPattern(datePattern) + ")";
            default:
                throw new UncheckedGcsConnectorException("Column type '" + columnType + "' is not supported for a partition column in this collector");
        }
    }

    private static String getDateRegExByPattern(String datePattern)
    {
        if (datePattern == null || datePattern.isBlank()) {
            return datePattern;
        }
        return datePattern.replaceAll("[YyMDdFEHhkKmswWS]", "\\\\d")
                .replaceAll("'", "") // replace ' from 'T'
                .replaceAll("Z", "-\\\\d{3,4}") // replace time-zone offset with 3-4 digits with the prefix '-'
                .replaceAll("z", "(.*?){2,6}") // replace time zone abbreviations with any character of length of min 2, max 6 (currently max is 5)
                .replaceAll("G", "AD"); // Era designator. Currently BC not supported
    }
}
