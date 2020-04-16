/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;

// AWS SDK ES 1.x
import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClientBuilder;
import com.amazonaws.services.elasticsearch.model.DomainInfo;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesResult;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsResult;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import com.amazonaws.services.elasticsearch.model.ElasticsearchDomainStatus;

// Guava
import com.google.common.base.Splitter;

// XML
import com.fasterxml.jackson.databind.ObjectMapper;

// Apache Arrow
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

// Elasticsearch
import org.elasticsearch.client.RestHighLevelClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Common
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Base64;

class ElasticsearchHelper {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHelper.class);

    // Env. variable that indicates whether the service is with Amazon ES Service (true) and thus the domain-
    // names and associated endpoints can be auto-discovered via the AWS ES SDK. Or, the Elasticsearch service
    // is external to Amazon (false), and the domain_mapping environment variable should be used instead.
    private static final String AUTO_DISCOVER_ENDPOINT = "auto_discover_endpoint";
    private static boolean autoDiscoverEndpoint;

    // Env. variable that holds the mappings of the domain-names to their respective endpoints. The contents of
    // this environment variable is fed into the domainSplitter to populate the domainMap where the key = domain-name,
    // and the value = endpoint.
    private static final String DOMAIN_MAPPING = "domain_mapping";

    // Splitter for inline map properties extracted from the DOMAIN_MAPPING.
    private static final Splitter.MapSplitter domainSplitter = Splitter.on(",").trimResults().withKeyValueSeparator("=");

    // A Map of the domain-names and their respective endpoints.
    private static final Map<String, String> domainMap = new HashMap<>();

    // Used when autoDiscoverEndpoint is false to create a client injected with username/password credentials.
    private static String username = "", password = "";

    // Used in parseMapping() to store the _meta structure (the mapping containing the fields that should be
    // considered a list).
    private static LinkedHashMap<String, Object> meta = new LinkedHashMap<>();

    // Used in parseMapping() to build the schema recursively.
    private static SchemaBuilder builder;

    /**
     * isAutoDiscoverEndpoint
     *
     * @return the boolean value of autoDiscoverEndpoint.
     */
    public static final boolean isAutoDiscoverEndpoint()
    {
        return autoDiscoverEndpoint;
    }

    /**
     * setDomainMapping(String, String)
     *
     * Populates the domainMap with domainName and domainEndpoint.
     * NOTE: Only gets called from @Test
     */
    @VisibleForTesting
    public static final void setDomainMapping(String domainName, String domainEndpoint)
    {
        // NOTE: Only gets called from @Test.
        domainMap.put(domainName, domainEndpoint);
    }

    /**
     * setDomainMapping(List<ElasticsearchDomainStatus>)
     *
     * Sets the domainMap with domain-names and corresponding endpoints retrieved from the AWS ES SDK.
     *
     * @param domainStatusList is a list of status objects returned by a listDomainNames request to the AWS ES SDK.
     */
    private static final void setDomainMapping(List<ElasticsearchDomainStatus> domainStatusList)
    {
        domainMap.clear();
        for (ElasticsearchDomainStatus domainStatus : domainStatusList) {
            domainMap.put(domainStatus.getDomainName(), domainStatus.getEndpoint());
        }
    }

    /**
     * setDomainMapping(Map<String, String>)
     *
     * Sets the domainMap with domain-names and corresponding endpoints stored in a Map object.
     *
     * @param mapping is a map of domain-names and their corresponding endpoints.
     */
    private static final void setDomainMapping(Map<String, String> mapping)
    {
        domainMap.clear();
        domainMap.putAll(mapping);
    }

    /**
     * setDomainMapping()
     *
     * Populates the domainMap with domain-mapping from either the domain_mapping environment variable
     * (auto_discover_endpoint is false), or from the AWS ES SDK (auto_discover_endpoint is true).
     * This method is called from the constructor(s) and from doListSchemaNames(). The call from the latter is used to
     * refresh the domainMap with the underlying assumption that domain(s) could have been added or deleted.
     */
    public static final void setDomainMapping()
    {
        logger.info("setDomainMapping: enter");

        autoDiscoverEndpoint = System.getenv(AUTO_DISCOVER_ENDPOINT) != null &&
                System.getenv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");

        if (autoDiscoverEndpoint) {
            // Get domain mapping via the AWS ES SDK (1.x).
            // NOTE: Gets called at construction and each call to doListSchemaNames() when autoDiscoverEndpoint is true.

            // AWS ES Client for retrieving domain mapping info from the Amazon Elasticsearch Service.
            AWSElasticsearch awsEsClient = AWSElasticsearchClientBuilder.defaultClient();

            try {
                ListDomainNamesResult listDomainNamesResult = awsEsClient.listDomainNames(new ListDomainNamesRequest());
                List<String> domainNames = new ArrayList<>();
                for (DomainInfo domainInfo : listDomainNamesResult.getDomainNames()) {
                    domainNames.add(domainInfo.getDomainName());
                }

                DescribeElasticsearchDomainsRequest describeDomainsRequest = new DescribeElasticsearchDomainsRequest();
                describeDomainsRequest.setDomainNames(domainNames);
                DescribeElasticsearchDomainsResult describeDomainsResult =
                        awsEsClient.describeElasticsearchDomains(describeDomainsRequest);

                setDomainMapping(describeDomainsResult.getDomainStatusList());
            }
            catch (Exception error) {
                logger.error("Error getting list of domain names:", error);
            }
            finally {
                awsEsClient.shutdown();
            }
        }
        else {
            // Get domain mapping from environment variables.
            String domainMapping = System.getenv(DOMAIN_MAPPING);
            if (domainMapping != null) {
                setDomainMapping(domainSplitter.split(domainMapping));
                setUsernamePassword();
            }
        }
    }

    /**
     * setUsernamePassword
     *
     * Sets the username and password from Secrets Manager. Called from setDomainMapping()
     * when autoDiscoverEndpoint is false.
     */
    private static final void setUsernamePassword()
    {
        logger.info("setUsernamePassword: enter");

        AWSSecretsManager awsSecretsManager = AWSSecretsManagerClientBuilder.standard().
                withCredentials(new DefaultAWSCredentialsProviderChain()).build();

        try {
            final String secretName = "elasticsearch-creds";
            GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);
            GetSecretValueResult getSecretValueResult;
            getSecretValueResult = awsSecretsManager.getSecretValue(getSecretValueRequest);
            String secretString;

            if (getSecretValueResult.getSecretString() != null) {
                secretString = getSecretValueResult.getSecretString();
            } else {
                secretString = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
            }

            Map<String, String> secrets = new ObjectMapper().readValue(secretString, HashMap.class);
            username = secrets.get("username");
            password = secrets.get("password");
        }
        catch (Exception error) {
            logger.error("Error accessing Secrets Manager:", error);
        }
        finally {
            awsSecretsManager.shutdown();
        }
    }

    /**
     * getDomainList
     *
     * Gets a list of domain names.
     *
     * @return a list of domain-names stored in the domainMap.
     */
    public static final Set<String> getDomainList()
    {
        return domainMap.keySet();
    }

    /**
     * getDomainEndpoint
     *
     * Gets the domain's endpoint.
     *
     * @param domainName is the name associated with the endpoint.
     * @return a String containing the endpoint associated with the domainName, or an empty String if the domain-
     * name doesn't exist in the map.
     */
    public static final String getDomainEndpoint(String domainName)
    {
        String endpoint = "";

        if (domainMap.containsKey(domainName)) {
            endpoint = domainMap.get(domainName);
        }

        return endpoint;
    }

    /**
     * getClient
     *
     * Returns a client based on the autoDiscoverEndpoint environment variable.
     *
     * @param endpoint is the Elasticsearch cluster's domain endpoint.
     * @return an Elasticsearch Rest client. If autoDiscoverEndpoint = true, the client is injected
     *          with AWS Credentials. If autoDiscoverEndpoint = false and username/password are not
     *          empty, it is injected with username/password credentials. Otherwise a default client
     *          with no credentials is returned.
     */
    public static RestHighLevelClient getClient(String endpoint)
    {
        if (autoDiscoverEndpoint) {
            // For Amazon Elasticsearch service use client with injected AWS Credentials.
            return new AwsRestHighLevelClient.Builder(endpoint).
                    setCredentials(new DefaultAWSCredentialsProviderChain()).build();
        }
        else if (!username.isEmpty() && !password.isEmpty()) {
            // Create a client injected with username/password credentials.
            return new AwsRestHighLevelClient.Builder(endpoint).
                    setCredentials(username, password).build();
        }

        // Default client.
        return new AwsRestHighLevelClient.Builder(endpoint).build();
    }

    /**
     * toArrowType
     *
     * Convert the data type from Elasticsearch to Arrow.
     *
     * @param elasticType is the Elasticsearch datatype.
     * @return an ArrowType corresponding to the Elasticsearch type (default value is a VARCHAR).
     */
    public static ArrowType toArrowType(String elasticType)
    {
        switch (elasticType) {
            // case "text":
            // case "keyword":
            //    return Types.MinorType.VARCHAR.getType();
            case "long":
                return Types.MinorType.BIGINT.getType();
            case "integer":
                return Types.MinorType.INT.getType();
            case "short":
                return Types.MinorType.SMALLINT.getType();
            case "byte":
                return Types.MinorType.TINYINT.getType();
            case "double":
            case "scaled_float":
                return Types.MinorType.FLOAT8.getType();
            case "float":
            case "half_float":
                return Types.MinorType.FLOAT4.getType();
            case "date":
            case "date_nanos":
                return Types.MinorType.DATEMILLI.getType();
            case "boolean":
                return Types.MinorType.BIT.getType();
            case "binary":
                return Types.MinorType.VARBINARY.getType();
            default:
                return Types.MinorType.VARCHAR.getType();
        }
    }

    /**
     * parseMapping
     *
     * Parses the response to GET index/_mapping recursively to derive the index's schema.
     *
     * @param prefix is the parent field names in the mapping structure. The final field-name will be
     *               a concatenation of the prefix and the current field-name (e.g. 'address.zip').
     * @param mapping is the current map of the element in question (e.g. address).
     */
    private static final void parseMapping(String prefix, LinkedHashMap<String, Object> mapping)
    {
        for (String key : mapping.keySet()) {
            String fieldName = prefix.isEmpty() ? key : prefix + "." + key;
            LinkedHashMap<String, Object> currMapping = (LinkedHashMap<String, Object>) mapping.get(key);

            if (currMapping.containsKey("properties")) {
                parseMapping(fieldName, (LinkedHashMap<String, Object>) currMapping.get("properties"));
            }
            else if (currMapping.containsKey("type")) {
                Field field = new Field(fieldName,
                        FieldType.nullable(toArrowType((String) currMapping.get("type"))), null);

                if (meta.containsKey(fieldName)) {
                    builder.addField(new Field(fieldName, FieldType.nullable(Types.MinorType.LIST.getType()),
                            Collections.singletonList(field)));
                }
                else {
                    builder.addField(field);
                }
            }
        }
    }

    /**
     * parseMapping
     *
     * Main parsing method for the GET <index>/_mapping request.
     *
     * @param mapping is the structure that contains the mapping for all elements for the index.
     * @param _meta is the structure in the mapping containing the fields that should be considered a list.
     * @return a Schema derived from the mapping.
     */
    public static final Schema parseMapping(LinkedHashMap<String, Object> mapping, LinkedHashMap<String, Object> _meta)
    {
        logger.info("parseMapping: enter");

        builder = SchemaBuilder.newBuilder();
        String fieldName = "";
        meta.clear();
        meta.putAll(_meta);

        if (mapping.containsKey("properties")) {
            parseMapping(fieldName, (LinkedHashMap<String, Object>) mapping.get("properties"));
        }

        return builder.build();
    }
}
