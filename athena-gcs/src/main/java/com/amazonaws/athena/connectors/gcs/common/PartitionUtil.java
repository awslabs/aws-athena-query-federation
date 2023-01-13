/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;

public class PartitionUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUtil.class);

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
    /**
     * Match any alpha-num characters. Used to match VARCHAR type only partition keys
     */
    private static final String VARCHAR_OR_STRING_REGEX = "([a-zA-Z0-9]+)";

    private PartitionUtil()
    {
    }

    /**
     * Return a regular expression for partition pattern from AWS Glue. This will dynamically generate a
     * regular expression to match a folder within the GCS to see if the folder conforms with the partition keys
     * already setup in the AWS Glue Table (if any)
     *
     * @param table response of get table from AWS Glue
     * @return optional Sting of regular expression
     */
    public static Optional<String> getRegExExpression(Table table)
    {
        List<Column> partitionColumns = table.getPartitionKeys();
        validatePartitionColumnTypes(partitionColumns);
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
                folderMatchingPatternBuilder.append(getFolderValuePattern(partitionColumns, folderPart)).append("/");
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
     * @param partitionPattern Name of the bucket
     * @param folderModel      partition folder name
     * @param folderNameRegEx  folder name regular expression
     * @param partitionColumns partition column name list
     * @return List of storage partition(column name, column type and value)
     */
    public static List<PartitionColumnData> getStoragePartitions(String partitionPattern, String folderModel, String folderNameRegEx, List<Column> partitionColumns)
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

                    Optional<PartitionColumnData> optionalStoragePartition = produceStoragePartition(partitionColumns, partitionColumn, columnValue);
                    optionalStoragePartition.ifPresent(partitions::add);
                }
            }
        }
        return partitions;
    }

    // helpers
    /**
     * Validates partition column types. As of now, only VARCHAR (string or varchar in Glue Table)
     * @param columns List of Glue Table's colums
     */
    private static void validatePartitionColumnTypes(List<Column> columns)
    {
        for (Column column : columns) {
            String columnType = column.getType();
            LOGGER.info("validatePartitionColumnTypes - Field type of {} is {}", column.getName(), columnType);
            switch (columnType) {
                case "string":
                case "varchar":
                    return;
                default:
                    throw new IllegalArgumentException("Field type '" + columnType + "' is not supported for a partition field in this connector. " +
                            "Supported partition field type is VARCHAR (string or varchar in a Glue Table Schema)");
            }
        }
    }
    /**
     * Return a true when storage partition added successfully
     *
     * @param columns         list of column
     * @param columnName      Name of the partition column
     * @param columnValue     value of partition folder
     * @return boolean flag
     */
    private static Optional<PartitionColumnData> produceStoragePartition(List<Column> columns, String columnName, String columnValue)
    {
        if (columnValue != null && !columnValue.isBlank() && !columns.isEmpty()) {
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    return Optional.of(new PartitionColumnData(column.getName(), column.getType(), columnValue));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Return a dynamic folder regex pattern for the given folder part
     *
     * @param partitionColumns list of partition column to see column name and types for the folder regex pattern
     * @param folderPart       folder part string, each parts in a path is separated by a folder separator, usually a '/' (forward slash)
     * @return Regex pattern for the given folder part
     */
    private static String getFolderValuePattern(List<Column> partitionColumns, String folderPart)
    {
        Matcher partitionMatcher = PARTITION_PATTERN.matcher(folderPart);
        if (partitionMatcher.matches() && partitionMatcher.groupCount() > 1) {
            String variable = partitionMatcher.group(2);
            String columnName = variable.replaceAll("[{}]", "");
            for (Column column : partitionColumns) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    return createGroup(folderPart.replace(variable, VARCHAR_OR_STRING_REGEX), VARCHAR_OR_STRING_REGEX);
                }
            }
        }
        return folderPart;
    }

    /**
     * Return a folder pattern
     *
     * @param folderPattern Name of the bucket
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
     * Determine the partitions based on Glue Catalog
     *
     * @return A list of partitions
     */
    public static URI getPartitions(Table table, Map<String, FieldReader> fieldReadersMap) throws URISyntaxException
    {
        String locationUri;
        String tableLocation = table.getStorageDescriptor().getLocation();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        if (null != partitionPattern) {
            for (Map.Entry<String, FieldReader> field : fieldReadersMap.entrySet()) {
                partitionPattern = partitionPattern.replace("{" + field.getKey() + "}", field.getValue().readObject().toString());
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
