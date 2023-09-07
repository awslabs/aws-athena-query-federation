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
import org.apache.arrow.vector.FieldVector;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;

public class PartitionUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUtil.class);

    /**
     * Partition pattern regular expression to be used in compiling Pattern
     */
    private static final String PARTITION_PATTERN_REGEX = "\\$\\{(.*?)}";

    /**
     * Pattern from a regular expression that identifies a match in a phrases to see if there is any
     * partition key variable placeholder. A partition key variable placeholder looks something like the following:
     * year=${year}/month=${month}
     * Here, {year} and {month} are the partition key variable placeholders
     */
    private static final Pattern PARTITION_PATTERN = Pattern.compile(PARTITION_PATTERN_REGEX);

    /**
     * Match any alpha-num characters. Used to match VARCHAR type only partition keys
     */
    private static final String VARCHAR_OR_STRING_REGEX = "([a-zA-Z0-9_-\\s]+)";

    private PartitionUtil()
    {
    }

    /**
     * Returns a map of partition column names to their values
     *
     * @param table response of get table from AWS Glue
     * @param partitionFolder      partition folder name
     * @return Map<String, String> case insensitive partition column name -> partition column value
     */
    public static Map<String, String> getPartitionColumnData(Table table, String partitionFolder)
    {
        List<Column> partitionKeys = table.getPartitionKeys() == null ? com.google.common.collect.ImmutableList.of() : table.getPartitionKeys();
        return getRegExExpression(table).map(folderNameRegEx ->
            getPartitionColumnData(table.getParameters().get(PARTITION_PATTERN_KEY), partitionFolder, folderNameRegEx, partitionKeys))
            .orElse(com.google.common.collect.ImmutableMap.of());
    }

    /**
     * Return a list of storage partition(column name, column type and value)
     *
     * @param partitionPattern Name of the bucket
     * @param partitionFolder  partition folder name
     * @param folderNameRegEx  folder name regular expression
     * @param partitionColumns partition column name list
     * @return Map<String, String> case insensitive partition column name -> partition column value map
     */
    protected static Map<String, String> getPartitionColumnData(String partitionPattern, String partitionFolder, String folderNameRegEx, List<Column> partitionColumns)
    {
        Map<String, String> partitions = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Matcher partitionPatternMatcher = PARTITION_PATTERN.matcher(partitionPattern);
        Matcher partitionFolderMatcher = Pattern.compile(folderNameRegEx).matcher(partitionFolder);
        java.util.TreeSet<String> partitionColumnsSet = partitionColumns.stream()
                .map(c -> c.getName())
                .collect(Collectors.toCollection(() -> new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
        while (partitionFolderMatcher.find()) {
            for (int j = 1; j <= partitionFolderMatcher.groupCount() && partitionPatternMatcher.find(); j++) {
                LOGGER.debug("Partition folder {} : {}", partitionPatternMatcher.group(1), partitionFolderMatcher.group(j));
                if (partitionColumnsSet.contains(partitionPatternMatcher.group(1))) {
                    partitions.put(partitionPatternMatcher.group(1), partitionFolderMatcher.group(j));
                }
                else {
                    throw new IllegalArgumentException("Column '" + partitionPatternMatcher.group(1) + "' is not defined as partition key in Glue Table");
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
            String columnType = column.getType().toLowerCase();
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
     * Return a regular expression for partition pattern from AWS Glue. This will dynamically generate a
     * regular expression to match a folder within the GCS to see if the folder conforms with the partition keys
     * already setup in the AWS Glue Table (if any)
     *
     * @param table response of get table from AWS Glue
     * @return optional Sting of regular expression
     */
    protected static Optional<String> getRegExExpression(Table table)
    {
        List<Column> partitionColumns = table.getPartitionKeys() == null ? com.google.common.collect.ImmutableList.of() : table.getPartitionKeys();
        validatePartitionColumnTypes(partitionColumns);
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        // Check to see if there is a partition pattern configured for the Table by the user
        // if not, it returns empty value
        if (partitionPattern == null || StringUtils.isBlank(partitionPattern)) {
            return Optional.empty();
        }
        return Optional.of(partitionPattern.replaceAll(PARTITION_PATTERN_REGEX, VARCHAR_OR_STRING_REGEX));
    }

    /**
     * Determine the partition folder URI based on Table's partition.pattern and value retrieved from partition field reader (form readWithConstraint() method of GcsRecordHandler)
     * For example, for the following partition.pattern of the Glue Table:
     * <p>/folderName1=${partitionKey1}</p>
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
    public static URI getPartitionsFolderLocationUri(Table table, List<FieldVector> fieldVectors, int readerPosition) throws URISyntaxException
    {
        String locationUri;
        String tableLocation = table.getStorageDescriptor().getLocation();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_KEY);
        if (null != partitionPattern) {
            for (FieldVector fieldVector : fieldVectors) {
                fieldVector.getReader().setPosition(readerPosition);
                partitionPattern = partitionPattern.replace("${" + fieldVector.getName() + "}", fieldVector.getReader().readObject().toString());
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
