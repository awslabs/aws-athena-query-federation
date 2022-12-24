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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Collectors;

// NOTE: Spark dsv2 adapters for Athena Federation must implement this class
// Implementations of this class must also be java.io.Serializable
public interface AthenaFederationAdapterDefinition
{
    // These must be public static final since they are defined in an interface
    public static final String DefaultConfigPrefix = "athena.connectors.conf.";
    public static final String DefaultSchemaKey = "athena.connectors.schema";
    public static final String DefaultTableKey = "athena.connectors.table";
    public static final String DefaultAccountKey = "athena.connectors.account";

    // Implementations of this class must implement these methods:
    public MetadataHandler getMetadataHandler(Map<String, String> configOptions);
    public RecordHandler getRecordHandler(Map<String, String> configOptions);

    // Default implementations that connectors are free to re-implement as necessary:
    // Implementers should not assume that configOptions is case insensitive
    public default Map<String, String> getFederationConfig(Map<String, String> configOptions)
    {
        // Find all the keys that start with our prefix and then strip off the prefix for the
        // config map that will be sent down to the connector.
        return configOptions.entrySet().stream()
            .filter(e -> e.getKey().startsWith(DefaultConfigPrefix))
            .map(e -> new SimpleImmutableEntry<String, String>(e.getKey().substring(DefaultConfigPrefix.length()), e.getValue()))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    public default TableName getTableName(Map<String, String> configOptions)
    {
        return new TableName(configOptions.get(DefaultSchemaKey), configOptions.get(DefaultTableKey));
    }

    /**
     * FederatedIdentity is currently not officially supported in Spark DSV2 Adapters.
     */
    public default FederatedIdentity getFederatedIdentity(Map<String, String> configOptions)
    {
        // This account is used for retrieving Glue Catalogs, so the user must
        // pass in an account when using cross-account Glue Catalogs.
        String account = configOptions.get(DefaultAccountKey);
        if (account == null) {
            // Otherwise, we just use the account from the current identity
            // NOTE: configOptions.getOrDefault() is purposefully not used here because
            // it is not lazy for the default parameter and there is no lazy equivalent
            // (computeIfAbsent is not the lazy equivalent since it will attempt to mutate the map).
            account = AWSSecurityTokenServiceClientBuilder.defaultClient()
                .getCallerIdentity(new GetCallerIdentityRequest())
                .getAccount();
        }

        // Basically all of these fields of FederatedIdentity are unused by the SDK and
        // connectors other than the "account" field, which is used by the GlueMetadataHandler.
        // See these results:
        //    git grep "\(get\)\?[iI]dentity\(()\)\?\.get" | grep -v "examples" | grep -v "/serde/"
        return new FederatedIdentity(
            "AthenaFederationUnusedARN",
            account,
            com.google.common.collect.ImmutableMap.of(),
            com.google.common.collect.ImmutableList.of());
    }

    public default String getQueryId(Map<String, String> configOptions)
    {
        return "AthenaFederationUnusedQueryId";
    }

    // The "catalogName" is a field that is normally passed in by the Athena Engine denoting the Athena Engine
    // catalog that the query is for.
    // The "catalogName" is the name of the "Athena Datasource" within the "Athena Datasource Catalog".
    // In the case of Spark, customers are not using their already existing defined Lambda connectors.
    // Instead they are using the jar directly, so the "catalogName" has no meaning.
    //
    // Additionally, inspecting the code will also show that there is no actual usage of the "catalogName" field
    // other than returning it back in responses and also used for connectors that support
    // "connection string multiplexing" (which is again also only relevant for the Lambda situation).
    // "default" is used only out of caution and backwards compatibility if for some reason a user decides to build
    // a Spark Adapted Athena Federation Connector using one of the `*Multiplexing*Handler`s instead of the normal
    // `*MetadataHandler`s and `*RecordHandler`s.
    // In that situation it will just use the "default" connection string property in the "configOptions" map.
    // See this for reference: https://docs.aws.amazon.com/athena/latest/ug/connectors-mysql.html#connectors-mysql-parameters
    //
    // Again, we do not plan to officially support any of the `*Multiplexing*Handler`s as Spark Adapted Federation Connectors
    // but we do need to pass some string anyway, so we might as well just use "default".
    public default String getCatalogName(Map<String, String> configOptions)
    {
        return "default";
    }

    // TODO:
    // In the future if adapters need further customization, we can add other get* methods
    // here for any of the following methods and then also update places where these classes
    // are created to be created from these methods instead of directly new'ing them:
    //    getAthenaFederationTable(...)
    //    getAthenaFederationScanBuilder(...)
    //    getAthenaFederationBatch(...)
    //    getAthenaFederationScan(...)
    //    getAthenaFederationInputPartition(...)
    //    getAthenaFederationPartitionReaderFactory(...)
    //    getAthenaFederationRecordHandlerProvider(...)
    //    getAthenaFederationPartitionReader(...)
}
