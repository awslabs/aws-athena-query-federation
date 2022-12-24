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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class AthenaFederationScan implements Scan
{
    private final AthenaFederationAdapterDefinition federationAdapterDefinition;
    private final StructType sparkSchema;
    private final String getTableLayoutSerializedString;
    private final Map<String, String> properties;
    private final String description;

    public AthenaFederationScan(
        AthenaFederationAdapterDefinition federationAdapterDefinition,
        StructType sparkSchema,
        String getTableLayoutSerializedString,
        Map<String, String> properties,
        String description)
    {
        this.federationAdapterDefinition = federationAdapterDefinition;
        this.sparkSchema = sparkSchema;
        this.getTableLayoutSerializedString = getTableLayoutSerializedString;
        this.properties = properties;
        this.description = description;
    }

    @Override
    public StructType readSchema()
    {
        return sparkSchema;
    }

    @Override
    public String description()
    {
        // A description string of this scan, which may includes information like:
        // what filters are configured for this scan, what's the value of some important options like path, etc.
        return description;
    }

    // This method is re-entrant
    @Override
    public Batch toBatch()
    {
        return new AthenaFederationBatch(
            federationAdapterDefinition,
            properties,
            getTableLayoutSerializedString);
    }
}
