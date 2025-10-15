/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.metadataProvider;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.PartitionInfoResult;
import com.amazonaws.athena.connectors.lark.base.model.TableDirectInitialized;
import com.amazonaws.athena.connectors.lark.base.model.TableSchemaResult;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * MetadataProvider implementation that retrieves metadata based on pre-resolved mappings
 * loaded from Lark Base/Drive sources during initialization.
 */
public class LarkSourceMetadataProvider
{
    private static final Logger logger = LoggerFactory.getLogger(LarkSourceMetadataProvider.class);

    private final List<TableDirectInitialized> resolvedMappings;

    public LarkSourceMetadataProvider(List<TableDirectInitialized> resolvedMappings)
    {
        this.resolvedMappings = requireNonNull(resolvedMappings, "resolvedMappings cannot be null");
    }

    public Optional<TableSchemaResult> getTableSchema(GetTableRequest request)
    {
        logger.info("Lark Source Path: Attempting to get schema for {}", request.getTableName());
        Optional<TableDirectInitialized> mappingOpt = findMapping(request.getTableName());

        if (mappingOpt.isPresent()) {
            TableDirectInitialized mapping = mappingOpt.get();
            Schema schema = CommonUtil.buildSchemaFromLarkFields(mapping.columns());
            logger.info("Lark Source Path: Found mapping for {} with schema: {}", request.getTableName(), schema);
            return Optional.of(new TableSchemaResult(schema, Collections.emptySet()));
        }
        else {
            logger.info("Lark Source Path: No mapping found for {}", request.getTableName());
            return Optional.empty();
        }
    }

    public Optional<PartitionInfoResult> getPartitionInfo(TableName tableName)
    {
        logger.info("Lark Source Path: Attempting to get partition info for {}", tableName);
        Optional<TableDirectInitialized> mappingOpt = findMapping(tableName);

        if (mappingOpt.isPresent()) {
            TableDirectInitialized mapping = mappingOpt.get();
            String baseId = mapping.database().larkBaseId();
            String tableId = mapping.table().larkBaseId();
            List<AthenaFieldLarkBaseMapping> columns = mapping.columns();

            if (baseId == null || tableId == null || baseId.isEmpty() || tableId.isEmpty() || columns.isEmpty()) {
                logger.warn("Lark Source Path: Mapping found for {} but contains invalid IDs (Base='{}', Table='{}') or empty columns. Returning empty Optional.",
                        tableName, baseId, tableId);
                return Optional.empty();
            }

            return Optional.of(new PartitionInfoResult(baseId, tableId, columns));
        }
        else {
            logger.info("Lark Source Path: No mapping found for {}", tableName);
            return Optional.empty();
        }
    }

    /**
     * Finds the pre-resolved mapping for the given table name.
     * (Moved from BaseMetadataHandler)
     */
    private Optional<TableDirectInitialized> findMapping(TableName tableName)
    {
        String requestedSchemaLower = tableName.getSchemaName().toLowerCase();
        String requestedTableLower = tableName.getTableName().toLowerCase();
        return this.resolvedMappings.stream()
                .filter(m -> m.database().athenaName().equalsIgnoreCase(requestedSchemaLower) &&
                        m.table().athenaName().equalsIgnoreCase(requestedTableLower))
                .findFirst();
    }
}
