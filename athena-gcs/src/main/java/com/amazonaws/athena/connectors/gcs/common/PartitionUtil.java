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
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;
import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.getFolderName;

public class PartitionUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUtil.class);

    private static final Pattern PARTITION_PATTERN = Pattern.compile("(.*?)=(\\{.*?\\})");

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
        private static final Pattern FIELD_EQUAL_VALUE_PATTERN_SINGLE_QUOTED_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)=\'(.*?)(?<!\\\\)\'");
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

    public static Optional<String> getRegExExpression(Table table, Storage storage)
    {
        List<Column> partitions = table.getPartitionKeys();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_PATTERN);
        if (partitionPattern == null || partitionPattern.isBlank()) {
            return Optional.empty();
        }
        Matcher partitinoMatcher = PARTITION_PATTERN.matcher(partitionPattern);
        String folderMatchingPattern;
        if (partitinoMatcher.matches()) {
            // implement it
        }
        String prefix = table.getStorageDescriptor().getLocation();
        return Optional.empty();
    }

    // helpers
//    private static String substitutePartitionVariables(String pattern, List<Column>)
//    {
//        if ()
//    }
}
