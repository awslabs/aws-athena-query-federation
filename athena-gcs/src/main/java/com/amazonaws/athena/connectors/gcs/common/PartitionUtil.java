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
    private static final Pattern PARTITION_PATTERN = Pattern.compile("\\{(.*?)}");

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
        String folderMatchingRegexPattern = partitionPattern;
        while (partitionMatcher.find()) {
            folderMatchingRegexPattern = folderMatchingRegexPattern.replace("{" + partitionMatcher.group(1) + "}", VARCHAR_OR_STRING_REGEX);
        }
        return Optional.of(folderMatchingRegexPattern);
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
                    String partitionColumn;
                    String columnValue;
                    if (matcher.group(0).equals(matcher.group(1))) { // Non-hive partition (e.g, /{partitionKey1}//{partitionKey2})
                        Matcher nonHivePartitionPatternMatcher = PARTITION_PATTERN.matcher(partitionPatternParts[i]);
                        if (nonHivePartitionPatternMatcher.matches()) {
                            partitionColumn = nonHivePartitionPatternMatcher.group(1);
                        }
                        else { // unknown partition layout
                            throw new IllegalArgumentException("Unsupported partition layout pattern. partition.pattern is " + partitionPattern);
                        }
                    }
                    else if (matcher.group(0).contains("=")) { // Hive partition (e.g. /folderName1={partitionKey1}/folderName2={partitionKey2})
                        partitionColumn = matcher.group(0).substring(0, matcher.group(0).indexOf("="));
                    }
                    else { // mixed partition (e.g, /folderName{partitionKey})
                        Matcher mixedPartitionPatternMatcher = PARTITION_PATTERN.matcher(partitionPatternParts[i]);
                        if (mixedPartitionPatternMatcher.find()) {
                            partitionColumn = mixedPartitionPatternMatcher.group(1);
                        }
                        else { // unknown partition layout
                            throw new IllegalArgumentException("Unsupported partition layout pattern. partition.pattern is " + partitionPattern);
                        }
                    }
                    columnValue = matcher.group(1);
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
     * @param columns List of Glue Table's columns
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
        throw new IllegalArgumentException("Column '" + columnName + "' is not defined as partition ke in Glue Table");
    }

    /**
     * Determine the partition folder URI based on Table's partition.pattern and value retrieved from partition field reader (form readWithConstraint() method of GcsRecordHandler)
     * For example, for the following partition.pattern of the Glue Table:
     * <p>/folderName1={partitionKey1}</p>
     * And for the following partition row (from getPartitions() method in GcsMetadataHandler):
     * <p>
     *     Partition fields and value:
     *     <ul>
     *         <li>Partition column: folderName1</li>
     *         <li>Partition column value: asdf</li>
     *     </ul>
     * </p>
     * when the Table's Location URI is gs://my_table/data/
     * this method will return a URI that refer to the GCS location: gs://my_table/data/folderName1=asdf
     * @return Gcs location URI
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
