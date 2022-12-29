/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.connectors.gcs.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.CharMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class StorageUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageUtil.class);

    /**
     * Regular expression to remove invalid characters from entity name as per ANSI/ISO Entry SQL-92 standard, except
     * entity names in underlying storage specific connector are all lower case
     * Also replace invalid characters for file naming (for example, '/' characters is not valid for file naming)
     */
    private static final String INVALID_CHARS_REPLACE_REGEX = "[^A-Za-z0-9]+|[\\\\/:*?\"<>|]+";

    /**
     * Regular expression to see whether a string starts with number(s)
     */
    private static final String STARTS_DIGIT_REGEX = "^\\d+";

    /**
     * Character matchers to retain only alphanumeric characters and an underscore
     * It helps to replace any special characters from the string
     */
    private static final CharMatcher ALPHA_NUM_MATCHER =
            CharMatcher.inRange('a', 'z')
                    .or(CharMatcher.inRange('A', 'Z'))
                    .or(CharMatcher.inRange('0', '9')).precomputed()
                    .or(CharMatcher.is('_'));

    private StorageUtil()
    {
    }

    /**
     * Counts the CSV records with the help of {@link CsvRoutines#getInputDimension(File)}
     *
     * @return Record count in the CSV file
     */
//    @Deprecated
//    public static synchronized long getCsvRecordCount(File cachedFile)
//    {
//        InputDimension dimension = new CsvRoutines().getInputDimension(cachedFile);
//        // excluding the header row
//        return dimension.rowCount() - 1;
//    }
//
//    public static synchronized long getCsvRecordCount(InputStream inputStream)
//    {
//        InputDimension dimension = new CsvRoutines().getInputDimension(inputStream);
//        // excluding the header row
//        return dimension.rowCount() - 1;
//    }

    /**
     * Replaces invalid characters in SQL entity name and removes special characters (e.g, #, $ @, etc.) And also invalid characters for file naming
     * This also removes initial digit(s) from the resulting entity name. For example '9table' will become 'table' after removal
     * Before performing above action, it removes the file extension, if any found
     *
     * @param name      Name of the file
     * @param extension Extension of the file to ignore from the SQL entity name
     * @return New nae as a Table name without file extension and after escaping ANSI SQL compliant reserved characters
     */
    public static String getValidEntityNameFromFile(String name, String extension)
    {
        name = name.toLowerCase(Locale.ROOT);
        int extensionIndex = name.indexOf(extension.toLowerCase(Locale.ROOT));
        if (extensionIndex > -1 && name.length() > 1) {
            name = name.substring(0, extensionIndex);
        }
        return ALPHA_NUM_MATCHER.retainFrom(name.replaceAll(INVALID_CHARS_REPLACE_REGEX, "_").toLowerCase(Locale.ROOT))
                .replaceAll(STARTS_DIGIT_REGEX, "");
    }

    /**
     * Replaces invalid characters in SQL entity name and removes special characters (e.g, #, $ @, etc.) And also invalid characters for file naming
     * This also removes initial digit(s) from the resulting entity name. For example '9table' will become 'table' after removal
     *
     * @param entityName Name of the file
     * @return New nae as a Table name without file extension and after escaping ANSI SQL compliant reserved characters
     */
    public static synchronized String getValidEntityName(String entityName)
    {
        return ALPHA_NUM_MATCHER.retainFrom(entityName.replaceAll(INVALID_CHARS_REPLACE_REGEX, "_").toLowerCase(Locale.ROOT))
                .replaceAll(STARTS_DIGIT_REGEX, "");
    }

    public static String tableNameFromFile(String objectName, String extension)
    {
        LOGGER.debug("Create table name from a file {} with extension {}", objectName, extension);
        String strLowerObjectName = objectName.toLowerCase(Locale.ROOT);
        int toIndex = strLowerObjectName.lastIndexOf(extension.toLowerCase(Locale.ROOT));
        if (toIndex > 2) {
            return strLowerObjectName.substring(0, toIndex);
        }
        return objectName;
    }

    public static void printJson(Object object, String prefix)
    {
        LOGGER.debug("Printing json for {}:", prefix);
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            LOGGER.debug("{}", object);
        }
        catch (Exception exception) {
            // Ignored. Printing object instead as a fall-back
            LOGGER.debug("Printing object for {}:\n{}", prefix, object);
        }
    }

    public static String createUri(String bucketName, String objectNames)
    {
//        return "s3://" + datasourceConfig.getHmacKey() + ":" + datasourceConfig.getHmacSecret() + "@" + bucketName + "/" + objectNames
//                + "?endpoint_override=https%3A%2F%2Fstorage.googleapis.com";
        return "gs://" + bucketName + "/" + objectNames;
    }

    public static String createUri(String path)
    {
        return "gs://"  + path;
    }
}
