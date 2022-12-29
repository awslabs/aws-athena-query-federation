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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;
import static java.util.Objects.requireNonNull;

public class PartitionUtil
{
    private static final Pattern PARTITION_PATTERN = Pattern.compile("(.*)(\\{.*?})(.*?)");

    private static final String DEFAULT_DATE_REGEX_STRING = "\\d{4}-\\d{2}-\\d{2}";

    private PartitionUtil()
    {
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
            StringBuilder folderMatchingPatternBuilder = new StringBuilder();
            for (String folderPart : folderParts) {
                folderMatchingPatternBuilder.append(getFolderValuePattern(partitionColumns, folderPart, tableParameters)).append("/");
            }
            folderMatchingPattern = folderMatchingPatternBuilder.toString();
        }

        if (!folderMatchingPattern.isBlank() && !folderMatchingPattern.endsWith("/")) {
            folderMatchingPattern += "/";
        }

        if (!folderMatchingPattern.isBlank()) {
            String patternToCheck = folderMatchingPattern;
            if (patternToCheck.contains(DEFAULT_DATE_REGEX_STRING)) {
                patternToCheck = patternToCheck.replace(DEFAULT_DATE_REGEX_STRING, "");
            }
            if (patternToCheck.contains("{") || patternToCheck.contains("}")) {
                throw new UncheckedGcsConnectorException("partition.partition parameter is either invalid or contains a column variable " +
                        "which is not the part of partitions. Pattern is: " + partitionPattern);
            }
            return Optional.of(folderMatchingPattern.replaceAll("['\"]", ""));
        }
        return Optional.empty();
    }

    public static List<StoragePartition> getStoragePartitions(String partitionPattern, String folderModel, String folderNameRegEx, List<Column> partitionColumns, Map<String, String> tableParameters) throws ParseException
    {
        List<StoragePartition> partitions = new ArrayList<>();
        String[] partitionPatternParts = partitionPattern.split("/");
        String[] regExParts = folderNameRegEx.split("/");
        String[] folderParts = folderModel.split("/");
        if (folderParts.length >= regExParts.length) {
            for (int i = 0; i < regExParts.length; i++) {
                Matcher matcher = Pattern.compile(regExParts[i]).matcher(folderParts[i]);
                if (matcher.matches() && matcher.groupCount() > 0) {
                    String partitionColumn = null;
                    String columnValue = null;
                    if (matcher.groupCount() == 1
                            && matcher.group(0).equals(matcher.group(1))) {
                        Matcher nonHivePartitionPatternMatcher = PARTITION_PATTERN.matcher(partitionPatternParts[i]);
                        if (nonHivePartitionPatternMatcher.matches()) {
                            partitionColumn = nonHivePartitionPatternMatcher.group(2).replaceAll("[{}]", "");
                        }
                        else { // unknown partition layout
                            continue;
                        }
                        columnValue = matcher.group(1);
                    }
                    else if (matcher.groupCount() > 1) {
                        String columnName = matcher.group(1);
                        if (columnName.contains("=")) {
                            partitionColumn = matcher.group(1).replaceAll("=", "");
                        }
                        else {
                            Matcher nonHivePartitionPatternMatcher = PARTITION_PATTERN.matcher(partitionPatternParts[i]);
                            if (nonHivePartitionPatternMatcher.matches()) {
                                partitionColumn = nonHivePartitionPatternMatcher.group(2).replaceAll("[{}]", "");
                            }
                            else { // unknown partition layout
                                continue;
                            }
                        }
                        columnValue = matcher.group(2);
                    }

                    if (columnValue == null) {
                        continue;
                    }

                    StoragePartition partition = new StoragePartition();
                    if (setStoragePartitionValues(partitionColumns, partitionColumn,  columnValue, partition, tableParameters)) {
                        partitions.add(partition);
                    }
                }
            }
        }
        return partitions;
    }

    // helpers
    private static boolean setStoragePartitionValues(List<Column> columns, String columnName, String columnValue, StoragePartition partition, Map<String, String> tableParameters) throws ParseException
    {
        if (columnValue != null && !columnValue.isBlank()) {
            if (!columns.isEmpty()) {
                for (Column column : columns) {
                    if (column.getName().equalsIgnoreCase(columnName)) {
                        partition.columnName(column.getName())
                                .columnType(column.getType())
                                .columnValue(convertStringByColumnType(column.getName(), column.getType(), columnValue, tableParameters));
                        return true;
                    }
                }
            }
        }
        return false;
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
                throw new UncheckedGcsConnectorException("Column type '" + columnType + "' is not supported for a partition column in this connector");
        }
    }

    private static Object convertStringByColumnType(String columnName, String columnType, String columnValue, Map<String, String> tableParameters) throws ParseException
    {
        switch (columnType) {
            case "string":
            case "varchar":
                return columnValue;
            case "bigint":
                return Long.parseLong(columnValue);
            case "int":
            case "smallint":
            case "tinyint":
                return Integer.parseInt(columnValue);
            case "date":
                String datePattern = tableParameters.get(String.format("partition.%s.pattern", columnName));
                if (datePattern == null) {
                    datePattern = DEFAULT_DATE_REGEX_STRING;
                }
                return new SimpleDateFormat(datePattern).parse(columnValue);
            default:
                throw new UncheckedGcsConnectorException("Column type '" + columnType + "' is not supported for a partition column in this connector");
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
