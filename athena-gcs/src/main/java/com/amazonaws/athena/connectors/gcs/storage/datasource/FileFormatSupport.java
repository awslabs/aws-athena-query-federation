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
package com.amazonaws.athena.connectors.gcs.storage.datasource;

import java.util.List;

public class FileFormatSupport
{
    private FileFormatSupport()
    {
    }

    /**
     * SupportedFileFormat that supports parquet file. Also helps to instantiate corresponding datasource
     */
    private static final FileFormat.SupportedFileFormat PARQUET_FILE_FORMAT
            = new FileFormat.SupportedFileFormat("parquet", ParquetDatasource.class);

    /**
     * SupportedFileFormat that supports csv file. Also helps to instantiate corresponding datasource
     */
    private static final FileFormat.SupportedFileFormat CSV_FILE_FORMAT
            = new FileFormat.SupportedFileFormat("csv", CsvDatasource.class);

    /**
     * A list of supported file format. As of the current implementation, only PARQUET and CSV
     */
    private static final List<FileFormat.SupportedFileFormat> supportedFormats
            = List.of(PARQUET_FILE_FORMAT, CSV_FILE_FORMAT);

    /**
     * Retrieves supported {@link FileFormat.SupportedFileFormat} if it accepts the supplied extension
     *
     * @param extension The file extension
     * @return An instance of {@link FileFormat.SupportedFileFormat}
     */
    protected static FileFormat.SupportedFileFormat getSupportedFormat(String extension)
    {
        for (FileFormat.SupportedFileFormat supportedFileFormat : supportedFormats) {
            if (supportedFileFormat.accept(extension)) {
                return supportedFileFormat;
            }
        }
        return null;
    }
}
