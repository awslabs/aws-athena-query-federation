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

import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

public class AthenaFederationTable implements SupportsRead
{
    private final AthenaFederationAdapterDefinition federationAdapterDefinition;
    private final StructType sparkSchema;
    private final GetTableResponse getTableResponse;
    private final Transform[] partitionTransforms;

    public AthenaFederationTable(
        AthenaFederationAdapterDefinition federationAdapterDefinition,
        StructType sparkSchema,
        GetTableResponse getTableResponse,
        Transform[] partitionTransforms)
    {
        this.federationAdapterDefinition = federationAdapterDefinition;
        this.sparkSchema = sparkSchema;
        this.getTableResponse = getTableResponse;

        // TODO: Need to investigate what this is further.
        // So far it doesn't seem like its necessary.
        this.partitionTransforms = partitionTransforms;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        return new AthenaFederationScanBuilder(federationAdapterDefinition, sparkSchema, getTableResponse, options.asCaseSensitiveMap());
    }

    @Override
    public String name()
    {
        return getTableResponse.getTableName().getQualifiedTableName();
    }

    @Override
    public StructType schema()
    {
        return sparkSchema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return com.google.common.collect.ImmutableSet.of(TableCapability.BATCH_READ);
    }
}
