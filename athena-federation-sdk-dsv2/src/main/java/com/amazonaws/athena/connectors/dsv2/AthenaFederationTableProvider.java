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

import com.amazonaws.athena.connector.lambda.data.ArrowSchemaUtils;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

// NOTE: Spark dsv2 adapters for Athena Federation must implement this class
public abstract class AthenaFederationTableProvider implements TableProvider, AthenaFederationAdapterDefinitionProvider
{
    private GetTableResponse getTableResponse;
    private AthenaFederationAdapterDefinition federationAdapterDefinition;

    public AthenaFederationTableProvider()
    {
        // Disable timezone packing
        com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil.disableTimezonePacking();
        this.federationAdapterDefinition = getAthenaFederationAdapterDefinition();
        this.getTableResponse = null;
    }

    public static ArrowType mapTimeTypeUnitsToMicros(ArrowType input)
    {
        switch (input.getTypeID()) {
            // Arrow only supports dates with either days or milliseconds as
            // the internal unit, so convert Date to Timestamp
            case Date:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
            // For Duration we only need to convert the unit
            case Duration:
                return new ArrowType.Duration(TimeUnit.MICROSECOND);
            // For Timestamp we need to convert the unit and also specify the appropriate bit width.
            // From the cpp docs, it looks like the bit width should be 64 for microseconds
            // https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv46time64N8TimeUnit4typeE
            // Similarly in the java docs for TimeMicroHolder, the width is 8 bytes:
            // https://arrow.apache.org/docs/java/reference/constant-values.html#org.apache.arrow.vector.holders.TimeMicroHolder.WIDTH
            case Time:
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            // For Timestamps, we have to convert the unit and retain the timezone
            case Timestamp:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, ((ArrowType.Timestamp) input).getTimezone());
        }
        // Otherwise just return the arrow type untouched
        return input;
    }

    public static Schema convertSchemaMilliFieldsToMicro(Schema schema)
    {
       List<Field> updatedFields = StreamSupport.stream(schema.getFields().spliterator(), false)
          .map(f -> ArrowSchemaUtils.remapArrowTypesWithinField(f, AthenaFederationTableProvider::mapTimeTypeUnitsToMicros))
          .collect(Collectors.toList());
        return new Schema(updatedFields, schema.getCustomMetadata());
    }

    // AthenaFederationTableProvider is implemented with the assumption that the TableProvider is created per query.
    // This is important because we store the GetTableResponse from a doGetTable() invocation, which would
    // be different for different queries (and thus storing and returning it would be wrong if a single
    // AthenaFederationTableProvider instance is being used across multiple queries.
    // This assumption looks to be true given this class from Spark: org.apache.spark.sql.internal.connector.SimpleTableProvider
    // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/connector/SimpleTableProvider.scala
    private GetTableResponse loadGetTableResponse(Map<String, String> properties)
    {
        if (getTableResponse != null) {
            return getTableResponse;
        }

        try (BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator())) {
            // NOTE: Nothing to close for both GetTableRequest and GetTableResponse
            GetTableRequest getTableRequest = new GetTableRequest(
                federationAdapterDefinition.getFederatedIdentity(properties),
                federationAdapterDefinition.getQueryId(properties),
                federationAdapterDefinition.getCatalogName(properties),
                federationAdapterDefinition.getTableName(properties));

            // doGetTable will return the table schema with the partition columns included as part of the schema.
            // Also the arrow table schema may contain metadata that needs to be passed on to other calls down the chain.
            // NOTE: Not sure why doGetTable even needs a blockAllocator, I don't see it getting used in most of the doGetTable
            // implementations that I've looked at.
            getTableResponse = federationAdapterDefinition
                .getMetadataHandler(federationAdapterDefinition.getFederationConfig(properties))
                .doGetTable(blockAllocator, (GetTableRequest) getTableRequest);

            // Convert the schema fields as necessary
            Schema updatedSchema = convertSchemaMilliFieldsToMicro(getTableResponse.getSchema());
            getTableResponse = new GetTableResponse(
                getTableResponse.getCatalogName(),
                getTableResponse.getTableName(),
                updatedSchema,
                getTableResponse.getPartitionColumns());

            return getTableResponse;
        }
        catch (Exception ex) {
            // We have to rethrow as unchecked because the underlying interface does not declare any checked throws
            throw new RuntimeException(ex);
        }
    }

    // NOTE: options here seems to be the same thing as properties in getTable, but just case insensitive
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        // NOTE: StructType does not contain any of the metadata that was embedded in the table response arrow schema.
        // Methods like pruneColumns() needs to be aware of this when it converts from StructType back into an arrow schema.
        return ArrowUtils.fromArrowSchema(loadGetTableResponse(options.asCaseSensitiveMap()).getSchema());
    }

    @Override
    public Table getTable(StructType sparkSchema, Transform[] partitionTransforms, Map<String, String> properties)
    {
        return new AthenaFederationTable(
            federationAdapterDefinition,
            sparkSchema,
            loadGetTableResponse(properties),
            partitionTransforms);
    }

    @Override
    public boolean supportsExternalMetadata()
    {
        // External here means user specifed schemas on a DataFrameReader.
        // We don't support that, but we support user specified schemas from Glue (depending on the actual connector).
        return false;
    }
}
