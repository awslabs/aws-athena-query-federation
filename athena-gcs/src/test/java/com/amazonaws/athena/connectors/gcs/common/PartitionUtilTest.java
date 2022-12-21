package com.amazonaws.athena.connectors.gcs.common;

import com.amazonaws.athena.connectors.gcs.UncheckedGcsConnectorException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;
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

        List<Column> columns = List.of(
          createColumn("year", "bigint"),
          createColumn("month", "int")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
    }

    @Test(expected = UncheckedGcsConnectorException.class)
    public void testFolderNameRegExPatterExpectException()
    {
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "year={year}/birth_month{month}/{day}"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
    }

    @Test
    public void testFolderNameRegExPatter()
    {
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "year={year}/birth_month{month}/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        assertFalse("Expression shouldn't contain a '{' character", optionalRegEx.get().contains("{"));
        assertFalse("Expression shouldn't contain a '}' character", optionalRegEx.get().contains("}"));
    }

    @Test
    public void dynamicFolderExpressionWithDigits()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "state='Tamilnadu'/",
                "year=2000/birth_month10/",
                "zone=EST/",
                "year=2001/birth_month01/",
                "month01/"
        );
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "year={year}/birth_month{month}/"));
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        System.out.println(optionalRegEx.get());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
            else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithDefaultsDates() // failed
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "year=2000/birth_month10/",
                "creation_dt=2022-12-20/",
                "zone=EST/",
                "creation_dt=2012-01-01/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "creation_dt={creation_dt}/"));
        List<Column> columns = List.of(
                createColumn("creation_dt", "date")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        System.out.println(optionalRegEx.get());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
            else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithQuotedVarchar() // failed
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "year=2000/birth_month10/",
                "state='Tamilnadu'/",
                "zone=EST/",
                "state='UP'/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "state='{stateName}'/"));
        List<Column> columns = List.of(
                createColumn("stateName", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        System.out.println(optionalRegEx.get());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
            else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    @Test
    public void dynamicFolderExpressionWithUnquotedVarchar()
    {
        // Odd index matches, otherwise doesn't
        List<String> partitionFolders = List.of(
                "year=2000/birth_month10/",
                "state=Tamilnadu/",
                "zone=EST/",
                "state=UP/",
                "month01/"
        );
        // mock
        when(table.getParameters()).thenReturn(Map.of(PARTITION_PATTERN_PATTERN, "state={stateName}/"));
        List<Column> columns = List.of(
                createColumn("stateName", "string")
        );
        when(table.getPartitionKeys()).thenReturn(columns);
        // build regex
        Optional<String> optionalRegEx = PartitionUtil.getRegExExpression(table);
        assertTrue(optionalRegEx.isPresent());
        System.out.printf("Regular expression %s%n", optionalRegEx.get());
        Pattern folderMatchPattern = Pattern.compile(optionalRegEx.get());
        for (int i = 0; i < partitionFolders.size(); i++) {
            String folder = partitionFolders.get(i);
            if (i % 2 > 0) { // Odd, should match
                assertTrue("Folder " + folder + " didn't match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
            else { // Even shouldn't
                assertFalse("Folder " + folder + " should NOT match with the pattern", folderMatchPattern.matcher(folder).matches());
            }
        }
    }

    // helpers
    private static Column createColumn(String name, String type)
    {
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        return column;
    }
}
