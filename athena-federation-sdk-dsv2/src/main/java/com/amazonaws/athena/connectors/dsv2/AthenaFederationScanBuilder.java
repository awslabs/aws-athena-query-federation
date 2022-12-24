/*-
 * #%L
 * athena-federation-sdk-dsv2
 * %%
 * Copyright (C) 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.dsv2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AthenaFederationScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters
{
    private static final Logger logger = LoggerFactory.getLogger(AthenaFederationScanBuilder.class);

    private StructType sparkSchema;
    private GetTableResponse getTableResponse;
    private List<Filter> pushedFiltersList;
    private Map<String, ValueSet> constraintsMap;

    private final AthenaFederationAdapterDefinition federationAdapterDefinition;
    private final BlockAllocator blockAllocator;
    private final Map<String, String> properties;

    public AthenaFederationScanBuilder(
        AthenaFederationAdapterDefinition federationAdapterDefinition,
        StructType sparkSchema,
        GetTableResponse getTableResponse,
        Map<String, String> properties)
    {
        this.federationAdapterDefinition = federationAdapterDefinition;
        // blockAllocator is used in this class for allocation of Markers within the ValueSets that are generated for
        // the constraints.
        this.blockAllocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator());
        this.sparkSchema = sparkSchema;
        this.getTableResponse = getTableResponse;
        this.pushedFiltersList = new ArrayList();
        this.constraintsMap = new HashMap();
        this.properties = properties;
    }

    @Override
    public void pruneColumns(StructType requiredSchema)
    {
        // We always want to keep the partitioned columns since the federation sdk does this in the base MetadataHandler:
        //    Field partitionCol = request.getSchema().findField(nextPartCol);
        Set<String> fieldsToKeep = Stream.concat(
            Arrays.stream(requiredSchema.fieldNames()),
            getTableResponse.getPartitionColumns().stream()
        ).collect(Collectors.toSet());

        List<Field> updatedFields = getTableResponse.getSchema().getFields().stream()
            .filter(field -> fieldsToKeep.contains(field.getName()))
            .collect(Collectors.toList());

        // Generate a new arrow schema using the pruned fields + existing metadata
        Schema updatedArrowSchema = new Schema(updatedFields, getTableResponse.getSchema().getCustomMetadata());

        logger.info("Pruned schema: {}  ----  Original schema: {}", updatedArrowSchema, getTableResponse.getSchema());

        // Update the sparkSchema
        sparkSchema = ArrowUtils.fromArrowSchema(updatedArrowSchema);

        // Update the captured getTableResponse
        getTableResponse = new GetTableResponse(
            getTableResponse.getCatalogName(),
            getTableResponse.getTableName(),
            updatedArrowSchema,
            getTableResponse.getPartitionColumns());
    }

    @Override
    public Filter[] pushedFilters()
    {
        Filter[] ret = new Filter[pushedFiltersList.size()];
        return pushedFiltersList.toArray(ret);
    }

    // This method is re-entrant
    @Override
    public Filter[] pushFilters(Filter[] filters)
    {
        for (Filter filter : filters) {
            Set<String> referenceSet = Arrays.stream(filter.references()).collect(Collectors.toSet());
            if (referenceSet.size() != 1) {
                // We can only support filters that reference one column right now due to the way
                // `Constraints` and `ValueSet` are structured.
                logger.info("Skipping filter: {} because it has more than one reference: {}", filter, referenceSet);
                continue;
            }
            String columnName = filter.references()[0];
            try {
                ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, getTableResponse.getSchema(), blockAllocator);
                logger.info("Filter: {} compiled to ValueSet: {}", filter, valueSet);
                constraintsMap.merge(columnName, valueSet, (existingValueSet, newValueSet) -> {
                    logger.info("Merging existingValueSet with newValueSet: {} && {}", existingValueSet, newValueSet);
                    ValueSet merged = existingValueSet.intersect(blockAllocator, newValueSet);
                    logger.info("Merged ValueSet: {}", merged);
                    return merged;
                });
                pushedFiltersList.add(filter);
            }
            catch (Exception ex) {
                logger.warn("Exception while trying to convert filter for column: {}. Filter: {}. Exception: {}. Skipping.", columnName, filter, ex.toString());
            }
        }

        // The return value is to tell Spark what filters it should still evaluate.
        // Currently we return all filters back because we don't trust our connectors
        // to have proper pushdowns right now.
        // Spark will also know what filters we have pushed down but are still requesting
        // re-evaluation for, because it will call pushedFilters().
        // In the future when our pushdowns are more reliable, we can return only the filters
        // that we have not actually pushed down.
        return filters;
    }

    @Override
    public Scan build()
    {
        // Serialize the constraints and then close out the blockAllocator.
        //
        // This is inefficient but necessary since the Scan is used to produce
        // multiple batches and there is no close() on the Scan.
        //
        // So the only chance we get to close out the natively allocated memory
        // here for the Constraints is here.
        //
        // Then later AthenaFederationBatch will just deserialize the
        // constraints as necessary to pass into the various MetadataHandler
        // methods that it has to call.
        try {
            ObjectMapper objectMapper = VersionedObjectMapperFactory.create(blockAllocator);
            Constraints constraints = new Constraints(constraintsMap);
            GetTableLayoutRequest layoutReq = new GetTableLayoutRequest(
                federationAdapterDefinition.getFederatedIdentity(properties),
                federationAdapterDefinition.getQueryId(properties),
                getTableResponse.getCatalogName(),
                getTableResponse.getTableName(),
                constraints,
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());
            String getTableLayoutSerializedString = objectMapper.writeValueAsString(layoutReq);
            String description = String.format("Generated from: --- %s --- %s ---", getTableResponse, constraints);
            return new AthenaFederationScan(federationAdapterDefinition, sparkSchema, getTableLayoutSerializedString, properties, description);
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
            // We must rethrow unchecked because the underlying
            // ScanBuilder::build() interface does not declare any checked
            // exceptions.
            throw new RuntimeException(ex);
        }
        finally {
            this.blockAllocator.close();
        }
    }
}
