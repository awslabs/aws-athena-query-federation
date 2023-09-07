/*-
 * #%L
 * athena-gcs
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
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.createColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionUtilTest
{
    private Table table;

    @Before
    public void setup()
    {
        StorageDescriptor storageDescriptor = mock(StorageDescriptor.class);
        when(storageDescriptor.getLocation()).thenReturn("gs://mydatalake1test/birthday/");
        table = mock(Table.class);
        when(table.getStorageDescriptor()).thenReturn(storageDescriptor);

        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("year", "bigint"),
                createColumn("month", "int")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFolderNameRegExPatterExpectException()
    {
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, "year=${year}/birth_month${month}/${day}"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFolderNameRegExPatter()
    {
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, "year=${year}/birth_month${month}/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        assertFalse("Expression shouldn't contain a '{' character", optionalRegEx.get().contains("{"));
        assertFalse("Expression shouldn't contain a '}' character", optionalRegEx.get().contains("}"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void dynamicFolderExpressionWithDigits()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = com.google.common.collect.ImmutableList.of(
                "state='Tamilnadu'/",
                "year=2000/birth_month10/",
                "zone=EST/",
                "year=2001/birth_month01/",
                "month01/"
        );
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, "year=${year}/birth_month${month}/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void dynamicFolderExpressionWithDefaultsDates()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = com.google.common.collect.ImmutableList.of(
                "year=2000/birth_month10/",
                "creation_dt=2022-12-20/",
                "zone=EST/",
                "creation_dt=2012-01-01/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, "creation_dt=${creation_dt}/"));
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("creation_dt", "date")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test(expected = AssertionError.class)
    public void dynamicFolderExpressionWithQuotedVarchar() // failed
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = com.google.common.collect.ImmutableList.of(
                "year=2000/birth_month10/",
                "state=\"Tamilnadu\"/",
                "state='Tamilnadu'/",
                "zone=EST/",
                "state='UP'/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, "state='${stateName}'/"));
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("stateName", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithUnquotedVarchar()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = com.google.common.collect.ImmutableList.of(
                "year=2000/birth_month10/",
                "state=Tamilnadu/",
                "zone=EST/",
                "state=UP/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, "state=${stateName}/"));
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("stateName", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            } else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetHivePartitions()
    {
        String partitionPatten = "year=${year}/birth_month${month}/";
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, partitionPatten ));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Map<String, String> partitions = PartitionUtil.getPartitionColumnData(partitionPatten, "year=2000/birth_month09/", optionalRegEx.get(), table.getPartitionKeys());
        assertFalse("Map of partition values is empty", partitions.isEmpty());
        assertEquals("Partitions map size is more than 2", 2, partitions.size());
        // Assert partition 1
        assertEquals("First hive column name value doesn't match", partitions.get("year"), 2000L);

        // Assert partition 2
        assertEquals("Second hive column name value doesn't match", partitions.get("month"), 9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetHiveNonHivePartitions()
    {
        // mock
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("year", "bigint"),
                createColumn("month", "int"),
                createColumn("day", "int")
        );
        // mock
        when(table.getPartitionKeys()).thenReturn(columns);
        String partitionPatten = "year=${year}/birth_month${month}/${day}";
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, partitionPatten + "/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        Map<String, String> partitions = PartitionUtil.getPartitionColumnData(partitionPatten, "year=2000/birth_month09/12/",
                optionalRegEx.get(), table.getPartitionKeys());
        assertFalse("List of column prefix is empty", partitions.isEmpty());
        assertEquals("Partition size is more than 3", 3, partitions.size());
        // Assert partition 1
        assertEquals("First hive column name value doesn't match", partitions.get("year"), 2000L);

        // Assert partition 2
        assertEquals("Second hive column name value doesn't match", partitions.get("month"), 9);

        // Assert partition 3
        assertEquals("Third hive column name value doesn't match", partitions.get("day"), 12);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPartitionFolders()
    {
        // re-mock
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("year", "bigint"),
                createColumn("month", "int"),
                createColumn("day", "int")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        String partitionPattern = "year=${year}/birth_month${month}/${day}";

        // list of folders in a bucket
        List<String> bucketFolders = com.google.common.collect.ImmutableList.of(
                "year=2000/birth_month09/12/",
                "year=2000/birth_month09/abc",
                "year=2001/birth_month12/20/",
                "year=2001/",
                "year=2000/birth_month09/",
                "year=2000/birth_month/12",
                "stateName=2001/birthMonth11/15/"
        );

        // tests
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue("No regular expression found for the partition pattern", optionalRegEx.isPresent());
        Pattern folderMatchingPattern = Pattern.compile(optionalRegEx.get());
        for (String folder : bucketFolders) {
            if (folderMatchingPattern.matcher(folder).matches()) {
                Map<String, String> partitions = PartitionUtil.getPartitionColumnData(partitionPattern, folder, optionalRegEx.get(), table.getPartitionKeys());
                assertFalse("List of storage partitions is empty", partitions.isEmpty());
                assertEquals("Partition size is more than 3", 3, partitions.size());
            }
        }
    }

    @Test
    public void testHivePartition()
    {
        // re-mock
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("statename", "string"),
                createColumn("zipcode", "varchar")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        String partitionPattern = "StateName=${statename}/ZipCode=${zipcode}";
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, partitionPattern + "/"));
        // list of folders in a bucket
        List<String> bucketFolders = com.google.common.collect.ImmutableList.of(
                "StateName=WB/ZipCode=700099/",
                "year=2000/birth_month09/abc/",
                "StateName=TN/ZipCode=600001/",
                "year=2001/",
                "year=2000/birth_month09/",
                "year=2000/birth_month/12/",
                "/StateName=UP/ZipCode=226001/"
        );

        // tests
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue("No regular expression found for the partition pattern", optionalRegEx.isPresent());
        Pattern folderMatchingPattern = Pattern.compile(optionalRegEx.get());
        int matchCount = 0;
        for (String folder : bucketFolders) {
            if (folder.startsWith("/")) {
                folder = folder.substring(1);
            }
            if (folderMatchingPattern.matcher(folder).matches()) {
                Map<String, String> partitions = PartitionUtil.getPartitionColumnData(partitionPattern, folder, optionalRegEx.get(), table.getPartitionKeys());
                assertFalse("List of storage partitions is empty", partitions.isEmpty());
                assertEquals("Partition size is more than 2", 2, partitions.size());
                matchCount++;
            }
        }
        assertEquals("Match count should be 3", 3, matchCount);
    }

    @Test
    public void testNonHivePartition()
    {
        // re-mock
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("statename", "string"),
                createColumn("district", "varchar"),
                createColumn("zipcode", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        String partitionPattern = "${statename}/${district}/${zipcode}";
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, partitionPattern + "/"));
        // list of folders in a bucket
        List<String> bucketFolders = com.google.common.collect.ImmutableList.of(
                "WB/Kolkata/700099/",
                "year=2000/birth_month09/abc/",
                "TN/DistrictChennai/600001/",
                "year=2001/",
                "year=2000/birth_month09/",
                "year=2000/birth_month/12/",
                "UP/Lucknow/226001/"
        );

        // tests
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue("No regular expression found for the partition pattern", optionalRegEx.isPresent());
        Pattern folderMatchingPattern = Pattern.compile(optionalRegEx.get());
        int matchCount = 0;
        for (String folder : bucketFolders) {
            if (folderMatchingPattern.matcher(folder).matches()) {
                Map<String, String> partitions = PartitionUtil.getPartitionColumnData(partitionPattern, folder, optionalRegEx.get(), table.getPartitionKeys());
                assertFalse("List of storage partitions is empty", partitions.isEmpty());
                assertEquals("Partition size is more than 3", 3, partitions.size());
                matchCount++;
            }
        }
        assertEquals("Match count should be 3", 3, matchCount);
    }

    @Test
    public void testMixedLayoutStringOnlyPartition()
    {
        // re-mock
        List<Column> columns = com.google.common.collect.ImmutableList.of(
                createColumn("statename", "string"),
                createColumn("district", "varchar"),
                createColumn("zipcode", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        String partitionPattern = "StateName=${statename}/District${district}/${zipcode}";
        // mock
        when(table.getParameters()).thenReturn(com.google.common.collect.ImmutableMap.of(PARTITION_PATTERN_KEY, partitionPattern + "/"));
        // list of folders in a bucket
        List<String> bucketFolders = com.google.common.collect.ImmutableList.of(
                "StateName=WB/DistrictKolkata/700099/",
                "year=2000/birth_month09/abc/",
                "StateName=TN/DistrictChennai/600001/",
                "year=2001/",
                "year=2000/birth_month09/",
                "year=2000/birth_month/12/",
                "StateName=UP/DistrictLucknow/226001/"
        );

        // tests
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue("No regular expression found for the partition pattern", optionalRegEx.isPresent());
        Pattern folderMatchingPattern = Pattern.compile(optionalRegEx.get());
        int matchCount = 0;
        for (String folder : bucketFolders) {
            if (folderMatchingPattern.matcher(folder).matches()) {
                Map<String, String> partitions = PartitionUtil.getPartitionColumnData(partitionPattern, folder, optionalRegEx.get(), table.getPartitionKeys());
                assertFalse("List of storage partitions is empty", partitions.isEmpty());
                assertEquals("Partition size is more than 3", 3, partitions.size());
                matchCount++;
            }
        }
        assertEquals("Match count should be 3", 3, matchCount);
    }
}
