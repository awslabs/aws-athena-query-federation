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

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Table;
import org.apache.arrow.vector.complex.reader.FieldReader;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;
import static java.util.Objects.requireNonNull;

public class PartitionUtil
{
    /**
     * Pattern from a regular expression that identifies a match in a phrases to see if there is any
     * partition key variable placeholder. A partition key variable placeholder looks something like the following:
     * year={year}/month={month}
     * Here, {year} and {month} are the partition key variable placeholders
     */
    private static final Pattern PARTITION_PATTERN = Pattern.compile("(.*)(\\{.*?})(.*?)");

    /**
     * Regular expression to match a date in the pattern yyyy-MM-dd. For example this regex will match 2022-12-20,
     * 2023-01-05 but not 20-05-2022 (i.e., dd-MM-yyyy)
     */
    private static final String DEFAULT_DATE_REGEX_STRING = "\\d{4}-\\d{2}-\\d{2}";
    private static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    private PartitionUtil()
    {
    }

    /**
     * Return a regular expression for partition pattern from AWS Glue. This will dynamically generate a
     * regular expression to match a folder within the GCS to see if the folder conforms with the partition keys
     * already setup in the AWS Glue Table (if any)
     *
     * @param table  response of get table from AWS Glue
     * @return optional Sting of regular expression
     */
    public static Optional<String> getRegExExpression(Table table)
    {
        Map<String, String> tableParameters = table.getParameters();
        List<Column> partitionColumns = table.getPartitionKeys();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        // Check to see if there is a partition pattern configured for the Table by the user
        // if not, it returns empty value
        if (partitionPattern == null || partitionPattern.isBlank()) {
            return Optional.empty();
        }
        Matcher partitionMatcher = PARTITION_PATTERN.matcher(partitionPattern);
        String folderMatchingPattern = "";
        // Check to see if the pattern contains one or more partition key variable placeholders
        if (partitionMatcher.matches()) {
            String[] folderParts = partitionPattern.split("/");
            StringBuilder folderMatchingPatternBuilder = new StringBuilder();
            for (String folderPart : folderParts) {
                // Generates a dynamic regex for the folder part
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
                throw new IllegalArgumentException("partition.partition parameter is either invalid or contains a column variable " +
                        "which is not the part of partitions. Pattern is: " + partitionPattern);
            }
            return Optional.of(folderMatchingPattern.replaceAll("['\"]", ""));
        }
        return Optional.empty();
    }

    /**
     * Return a list of storage partition(column name, column type and value)
     *
     * @param partitionPattern  Name of the bucket
     * @param folderModel partition folder name
     * @param folderNameRegEx folder name regular expression
     * @param partitionColumns partition column name list
     * @param tableParameters table parameter
     * @return List of storage partition(column name, column type and value)
     */
    public static List<PartitionColumnData> getStoragePartitions(String partitionPattern, String folderModel, String folderNameRegEx, List<Column> partitionColumns, Map<String, String> tableParameters) throws ParseException
    {
        List<PartitionColumnData> partitions = new ArrayList<>();
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

                    PartitionColumnData partition = new PartitionColumnData();
                    if (setStoragePartitionValues(partitionColumns, partitionColumn,  columnValue, partition, tableParameters)) {
                        partitions.add(partition);
                    }
                }
            }
        }
        return partitions;
    }

    // helpers
    /**
     * Return a true when storage partition added successfully
     *
     * @param columns  list of column
     * @param columnName Name of the partition column
     * @param columnValue  value of partition folder
     * @param partition storage partition object
     * @param tableParameters table parameters
     * @return boolean flag
     */
    private static boolean setStoragePartitionValues(List<Column> columns, String columnName, String columnValue, PartitionColumnData partition, Map<String, String> tableParameters) throws ParseException
    {
        if (columnValue != null && !columnValue.isBlank() && !columns.isEmpty()) {
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    partition.columnName(column.getName())
                                .columnType(column.getType())
                                .columnValue(convertStringByColumnType(column.getName(), column.getType(), columnValue, tableParameters));
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Return a dynamic folder regex pattern for the given folder part
     *
     * @param partitionColumns  list of partition column to see column name and types for the folder regex pattern
     * @param folderPart folder part string, each parts in a path is separated by a folder separator, usually a '/' (forward slash)
     * @param tableParameters  Glue table parameters
     * @return Regex pattern for the given folder part
     */
    private static String getFolderValuePattern(List<Column> partitionColumns, String folderPart, Map<String, String> tableParameters)
    {
        Matcher partitionMatcher = PARTITION_PATTERN.matcher(folderPart);
        if (partitionMatcher.matches() && partitionMatcher.groupCount() > 1) {
            String variable = partitionMatcher.group(2);
            String columnName = variable.replaceAll("[{}]", "");
            for (Column column : partitionColumns) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    String regEx = requireNonNull(getRegExByColumnType(column.getName(), column.getType(), tableParameters));
                    return createGroup(folderPart.replace(variable, regEx), regEx);
                }
            }
        }
        return folderPart;
    }

    /**
     * Return a folder pattern
     *
     * @param folderPattern  Name of the bucket
     * @param variableRegEx Name of the file in the specified bucket
     * @return string folder pattern
     */
    private static String createGroup(String folderPattern, String variableRegEx)
    {
        int index = folderPattern.lastIndexOf(variableRegEx);
        if (index > 1) {
            return "(" + folderPattern.substring(0, index) + ")" + folderPattern.substring(index);
        }
        return folderPattern;
    }

    /**
     * Return a regEx column type
     *
     * @param columnName  Name of the column
     * @param columnType column type
     * @param tableParameters   table parameters
     * @return column type
     */
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
                return "(\\d+)";
            case "date":
                String datePattern = tableParameters.get(String.format("partition.%s.pattern", columnName));
                if (datePattern == null) {
                    return "(" + DEFAULT_DATE_REGEX_STRING + ")";
                }
                return "(" + getDateRegExByPattern(datePattern) + ")";
            default:
                throw new IllegalArgumentException("Column type '" + columnType + "' is not supported for a partition column in this connector");
        }
    }

    /**
     * Return a column value with exact type from string value
     *
     * @param columnName  Name of the column
     * @param columnType column type
     * @param tableParameters   table parameters
     * @return object of column value
     */
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
                    datePattern = DEFAULT_DATE_PATTERN;
                }
                return new SimpleDateFormat(datePattern).parse(columnValue);
            default:
                throw new IllegalArgumentException("Column type '" + columnType + "' is not supported for a partition column in this connector");
        }
    }

    /**
     * Return a date pattern string
     *
     * @param datePattern  value of partition folder when date pattern
     * @return string of date pattern
     */
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

    /**
     * Determine the partitions based on Glue Catalog
     * @return A list of partitions
     */
    public static URI getPartitions(Table table, Map<String, FieldReader> fieldReadersMap) throws URISyntaxException
    {
        String locationUri;
        String tableLocation = table.getStorageDescriptor().getLocation();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        if (null != partitionPattern) {
            for (Map.Entry<String, FieldReader> field : fieldReadersMap.entrySet()) {
                partitionPattern = partitionPattern.replace("{" + field.getKey() + "}", String.valueOf(field.getValue().readObject()));
            }
            locationUri = (tableLocation.endsWith("/")
                    ? tableLocation
                    : tableLocation + "/") + partitionPattern;
        }
        else {
            locationUri = tableLocation;
        }
        return new URI(locationUri);
    }
}
