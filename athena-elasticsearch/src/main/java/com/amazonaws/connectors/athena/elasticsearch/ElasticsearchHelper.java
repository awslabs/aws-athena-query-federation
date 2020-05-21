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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.services.elasticsearch.AWSElasticsearch;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClientBuilder;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsResult;
import com.amazonaws.services.elasticsearch.model.DomainInfo;
import com.amazonaws.services.elasticsearch.model.ElasticsearchDomainStatus;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesRequest;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesResult;
import com.google.common.base.Splitter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ElasticsearchHelper
{
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

    // Used when autoDiscoverEndpoint is false to create a client injected with username/password credentials
    // extracted from Amazon Secrets Manager.
    private static final String SECRET_NAME = "secret_name";
    private static String secretName;

    // Used in parseMapping() to store the _meta structure (the mapping containing the fields that should be
    // considered a list).
    private static LinkedHashMap<String, Object> meta = new LinkedHashMap<>();

    // Used in parseMapping() to build the schema recursively.
    private static SchemaBuilder builder;

    // Predicate conjunctions.
    private static final String AND_OPER = " AND ";
    private static final String OR_OPER = " OR ";
    private static final String EMPTY_PREDICATE = "";

    // Existence predicates.
    private static final String existsPredicate(boolean exists, String fieldName)
    {
        if (exists) {
            // (_exists:field)
            return "(_exists_:" + fieldName + ")";
        }
        else {
            // (NOT _exists_:field)
            return "(NOT _exists_:" + fieldName + ")";
        }
    }

    private ElasticsearchHelper() {}

    /**
     * Get an environment variable using System.getenv().
     * @param var is the environment variable.
     * @return the contents of the environment variable or an empty String if it's not defined.
     */
    public static final String getEnv(String var)
    {
        String result = System.getenv(var);

        return result == null ? "" : result;
    }

    /**
     * @return the boolean value of autoDiscoverEndpoint.
     */
    public static final boolean isAutoDiscoverEndpoint()
    {
        return autoDiscoverEndpoint;
    }

    /**
     * Gets a client factory that can create an Elasticsearch REST client injected with credentials.
     * @return a new client factory.
     */
    public static AwsRestHighLevelClientFactory getClientFactory()
    {
        logger.info("getClientFactory - enter");

        autoDiscoverEndpoint = getEnv(AUTO_DISCOVER_ENDPOINT).equalsIgnoreCase("true");
        secretName = getEnv(SECRET_NAME);

        if (autoDiscoverEndpoint) {
            // Client factory for clients injected with AWS credentials.
            return AwsRestHighLevelClientFactory.defaultFactory();
        }
        else {
            // Client factory for clients injected with username/password credentials.
            return new AwsRestHighLevelClientFactory.Builder().setUsernamePassword(secretName).build();
        }
    }

    /**
     * Sets the domainMap with domain-names and corresponding endpoints retrieved from the AWS ES SDK.
     * @param domainStatusList is a list of status objects returned by a listDomainNames request to the AWS ES SDK.
     */
    private static final void setDomainMapping(List<ElasticsearchDomainStatus> domainStatusList)
    {
        logger.info("setDomainMapping(List<>) - enter");

        domainMap.clear();
        for (ElasticsearchDomainStatus domainStatus : domainStatusList) {
            domainMap.put(domainStatus.getDomainName(), domainStatus.getEndpoint());
        }

        logger.info("setDomainMapping(List<>) - exit: " + domainMap);
    }

    /**
     * Sets the domainMap with domain-names and corresponding endpoints stored in a Map object.
     * @param mapping is a map of domain-names and their corresponding endpoints.
     */
    public static final void setDomainMapping(Map<String, String> mapping)
    {
        logger.info("setDomainMapping(Map<>) - enter");

        domainMap.clear();
        domainMap.putAll(mapping);

        logger.info("setDomainMapping(Map<>) - exit: " + domainMap);
    }

    /**
     * Populates the domainMap with domain-mapping from either the domain_mapping environment variable
     * (auto_discover_endpoint is false), or from the AWS ES SDK (auto_discover_endpoint is true).
     * This method is called from the constructor(s) and from doListSchemaNames(). The call from the latter is used to
     * refresh the domainMap with the underlying assumption that domain(s) could have been added or deleted.
     */
    public static void setDomainMapping()
    {
        logger.info("setDomainMapping - enter");

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
            // Get domain mapping from environment variable.
            String domainMapping = getEnv(DOMAIN_MAPPING);
            if (!domainMapping.isEmpty()) {
                setDomainMapping(domainSplitter.split(domainMapping));
            }
        }

        logger.info("setDomainMapping - exit");
    }

    /**
     * Gets a list of domain names.
     * @return a list of domain-names stored in the domainMap.
     */
    public static final Set<String> getDomainList()
    {
        return domainMap.keySet();
    }

    /**
     * Gets the domain's endpoint.
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
     * Allows for coercion of field-value types in the event that the returned type does not match the schema.
     * Multiple fields in Elasticsearch can be returned as a string, numeric (Integer, Long, Double), or null.
     * @param field is the field that we are coercing the value into.
     * @param fieldValue is the value to coerce
     * @return the coerced value.
     */
    public static Object coerceField(Field field, Object fieldValue)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        if (fieldType != Types.MinorType.LIST && fieldValue instanceof ArrayList) {
            // This is an abnormal case where the field was not defined as a list in the schema,
            // however, fieldValue returns a list => return first item in list only applying coercion
            // where necessary in order to match the type of the field being mapped into.
            return ElasticsearchHelper.coerceField(field, ((ArrayList) fieldValue).get(0));
        }

        switch (fieldType) {
            case LIST:
                return coerceListField(field, fieldValue);
            case BIGINT:
                if (field.getMetadata().size() > 0 && field.getMetadata().containsKey("scaling_factor")) {
                    // scaled_float w/scaling_factor - a float represented as a long.
                    double scalingFactor = new Double(field.getMetadata().get("scaling_factor")).doubleValue();
                    if (fieldValue instanceof String) {
                        return Math.round(new Double((String) fieldValue).doubleValue() * scalingFactor);
                    }
                    else if (fieldValue instanceof Number) {
                        return Math.round(((Number) fieldValue).doubleValue() * scalingFactor);
                    }
                    break;
                }
                else if (fieldValue instanceof String) {
                    return new Long((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).longValue();
                }
                break;
            case INT:
                if (fieldValue instanceof String) {
                    return new Integer((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).intValue();
                }
                break;
            case SMALLINT:
                if (fieldValue instanceof String) {
                    return new Short((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).shortValue();
                }
                break;
            case TINYINT:
                if (fieldValue instanceof String) {
                    return new Byte((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).byteValue();
                }
                break;
            case FLOAT8:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).doubleValue();
                }
                break;
            case FLOAT4:
                if (fieldValue instanceof String) {
                    return new Float((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).floatValue();
                }
                break;
            case DATEMILLI:
                if (fieldValue instanceof String) {
                    try {
                        // Date should be a formatted String: yyyy-mm-dd'T'hh:mm:ss.SSS (e.g. "2020-05-18T10:15:30.123").
                        // Nanoseconds will be rounded to the nearest millisecond.
                        LocalDateTime localDateTime = LocalDateTime.parse((String) fieldValue,
                                DateTimeFormatter.ISO_LOCAL_DATE_TIME.withResolverStyle(ResolverStyle.SMART));
                        double nanoSeconds = localDateTime.getNano();
                        return localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000 + Math.round(nanoSeconds / 1000000);
                    }
                    catch (Exception error) {
                        logger.error("Error parsing localDateTime value: " + fieldValue, error);
                        return null;
                    }
                }
                if (fieldValue instanceof Number) {
                    // Date should be a long numeric value representing epoch milliseconds (e.g. 1589525370001).
                    return ((Number) fieldValue).longValue();
                }
                break;
            case BIT:
                if (fieldValue instanceof String) {
                    return new Boolean((String) fieldValue);
                }
                break;
            default:
                break;
        }

        return fieldValue;
    }

     /**
     * Allows for coercion of a list of values where the returned types do not match the schema.
     * Multiple fields in Elasticsearch can be returned as a string, numeric (Integer, Long, Double), or null.
     * @param field is the field that we are coercing the value into.
     * @param fieldValue is the list of value to coerce
     * @return the coerced list of value.
     * @throws RuntimeException if the fieldType is not a LIST or the fieldValue is instanceof HashMap (STRUCT).
     */
    public static Object coerceListField(Field field, Object fieldValue)
            throws RuntimeException
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        switch (fieldType) {
            case LIST:
                Field childField = field.getChildren().get(0);
                if (fieldValue instanceof ArrayList) {
                    // Both fieldType and fieldValue are lists => Return as a new list of values, applying coercion
                    // where necessary in order to match the type of the field being mapped into.
                    List<Object> coercedValues = new ArrayList<>();
                    ((ArrayList) fieldValue).forEach(value ->
                            coercedValues.add(ElasticsearchHelper.coerceField(childField, value)));
                    return coercedValues;
                }
                else if (!(fieldValue instanceof HashMap)) {
                    // This is an abnormal case where the fieldType was defined as a list in the schema,
                    // however, the fieldValue returns as a single value => Return as a list of a single value
                    // applying coercion where necessary in order to match the type of the field being mapped into.
                    return Collections.singletonList(ElasticsearchHelper.coerceField(childField, fieldValue));
                }
                break;
            default:
                break;
        }

        throw new RuntimeException("Invalid field value encountered in Document for field: " + field.toString() +
                ",value: " + fieldValue.toString());
    }

    /**
     * Main parsing method for the GET <index>/_mapping request.
     * @param mapping is the structure that contains the mapping for all elements for the index.
     * @param metaMap is the structure in the mapping containing the fields that should be considered a list.
     * @return a Schema derived from the mapping.
     */
    public static Schema parseMapping(LinkedHashMap<String, Object> mapping, LinkedHashMap<String, Object> metaMap)
    {
        logger.info("parseMapping - enter");

        builder = SchemaBuilder.newBuilder();
        meta.clear();
        meta.putAll(metaMap);

        if (mapping.containsKey("properties")) {
            LinkedHashMap<String, Object> fields = (LinkedHashMap) mapping.get("properties");

            for (String fieldName : fields.keySet()) {
                builder.addField(inferField(fieldName, fieldName, (LinkedHashMap) fields.get(fieldName)));
            }
        }

        return builder.build();
    }

    /**
     * Parses the response to GET index/_mapping recursively to derive the index's schema.
     * @param fieldName is the name of the current field being processed (e.g. street).
     * @param qualifiedName is the qualified name of the field (e.g. address.street).
     * @param mapping is the current map of the element in question.
     * @return a Field object injected with the field's info.
     */
    protected static Field inferField(String fieldName, String qualifiedName, LinkedHashMap<String, Object> mapping)
    {
        Field field;

        if (mapping.containsKey("properties")) {
            // Process STRUCT.
            LinkedHashMap<String, Object> childFields = (LinkedHashMap) mapping.get("properties");
            List<Field> children = new ArrayList<>();

            for (String childField : childFields.keySet()) {
                children.add(inferField(childField, qualifiedName + "." + childField,
                        (LinkedHashMap) childFields.get(childField)));
            }

            field = new Field(fieldName, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }
        else {
            field = new Field(fieldName, toFieldType(mapping), null);

            if (meta.containsKey(qualifiedName)) {
                // Process LIST.
                return new Field(fieldName, FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(field));
            }
        }

        return field;
    }

    /**
     * Convert the data type from Elasticsearch to Arrow and injects it in a FieldType.
     * @param mapping is the map containing the Elasticsearch datatype.
     * @return a new FieldType corresponding to the Elasticsearch type.
     */
    public static FieldType toFieldType(LinkedHashMap<String, Object> mapping)
    {
        logger.info("toFieldType - enter: " + mapping);

        String elasticType = (String) mapping.get("type");
        Types.MinorType minorType;
        Map<String, String> metadata = new HashMap<>();

        switch (elasticType) {
            case "text":
            case "keyword":
            case "binary":
                minorType = Types.MinorType.VARCHAR;
                break;
            case "long":
                minorType = Types.MinorType.BIGINT;
                break;
            case "integer":
                minorType = Types.MinorType.INT;
                break;
            case "short":
                minorType = Types.MinorType.SMALLINT;
                break;
            case "byte":
                minorType = Types.MinorType.TINYINT;
                break;
            case "double":
                minorType = Types.MinorType.FLOAT8;
                break;
            case "scaled_float":
                minorType = Types.MinorType.BIGINT;
                metadata.put("scaling_factor", mapping.get("scaling_factor").toString());
                break;
            case "float":
            case "half_float":
                minorType = Types.MinorType.FLOAT4;
                break;
            case "date":
            case "date_nanos":
                minorType = Types.MinorType.DATEMILLI;
                break;
            case "boolean":
                minorType = Types.MinorType.BIT;
                break;
            default:
                minorType = Types.MinorType.NULL;
                break;
        }

        logger.info("Arrow Type: {}, metadata: {}", minorType.toString(), metadata);

        return new FieldType(true, minorType.getType(), null, metadata);
    }

    /**
     * Creates a projection (using the schema) on which fields should be included in the search index request. For
     * complex type STRUCT, there is no need to include each individual nested field in the projection. Since the
     * schema contains all nested fields in the STRUCT, only the name of the STRUCT field is added to the projection
     * allowing Elasticsearch to return the entire object including all nested fields.
     * @param schema is the schema containing the requested projection.
     * @return a projection wrapped in a FetchSourceContext object.
     */
    public static FetchSourceContext getProjection(Schema schema)
    {
        logger.info("getProjection - enter");

        List<String> includedFields = new ArrayList<>();

        for (Field field : schema.getFields()) {
            includedFields.add(field.getName());
        }

        logger.info("Included fields: " + includedFields.toString());

        return new FetchSourceContext(true, Strings.toStringArray(includedFields), Strings.EMPTY_ARRAY);
    }

    /**
     * Given a set of Constraints, create the query that can push predicates into the Elasticsearch data-source.
     * @param constraintSummary is a map containing the constraints used to form the predicate for predicate push-down.
     * @return the query builder that will be injected into the query.
     */
    public static QueryBuilder getQuery(Map<String, ValueSet> constraintSummary)
    {
        logger.info("getQuery - enter");

        List<String> predicates = new ArrayList<>();

        constraintSummary.forEach((fieldName, constraint) -> {
            String predicate = getPredicate(fieldName, constraint);
            if (!predicate.isEmpty()) {
                // predicate1, predicate2, predicate3...
                predicates.add(predicate);
            }
        });

        if (predicates.isEmpty()) {
            // No predicates formed.
            logger.info("Predicates are NOT formed.");
            return QueryBuilders.matchAllQuery();
        }

        // predicate1 AND predicate2 AND predicate3...
        String formedPredicates = Strings.collectionToDelimitedString(predicates, AND_OPER);
        logger.info("Formed Predicates: " + formedPredicates);

        return QueryBuilders.queryStringQuery(formedPredicates);
    }

    /**
     * Converts a single field constraint into a predicate to use in an Elasticsearch query.
     * @param fieldName The name of the field for the given ValueSet constraint.
     * @param constraint The constraint to apply to the given field.
     * @return A string describing the constraint for pushing down into Elasticsearch.
     */
    protected static String getPredicate(String fieldName, ValueSet constraint)
    {
        logger.info("getPredicate - enter\n\nField Name: {}\n\nConstraint: {}", fieldName, constraint);

        if (constraint.isNone()) {
            // (NOT _exists_:field)
            return existsPredicate(false, fieldName);
        }

        if (constraint.isAll()) {
            // (_exists:field)
            return existsPredicate(true, fieldName);
        }

        List<String> predicateParts = new ArrayList<>();

        if (!constraint.isNullAllowed()) {
            // null value should not be included in set of returned values => Include existence predicate.
            predicateParts.add(existsPredicate(true, fieldName));
        }

        if (constraint instanceof EquatableValueSet) {
            Block block = ((EquatableValueSet) constraint).getValues();
            List<String> singleValues = new ArrayList<>();
            FieldReader fieldReader = block.getFieldReaders().get(0);

            for (int i = 0; i < block.getRowCount(); i++) {
                singleValues.add(fieldReader.readObject().toString());
            }

            // field:(value1 OR value2 OR value3...)
            predicateParts.add(fieldName + ":(" + Strings.collectionToDelimitedString(singleValues, OR_OPER) + ")");
        }
        else {
            String rangedPredicate = getPredicateFromRange(fieldName, constraint);
            if (!rangedPredicate.isEmpty()) {
                predicateParts.add(rangedPredicate);
            }
        }

        return predicateParts.isEmpty() ? EMPTY_PREDICATE : Strings.collectionToDelimitedString(predicateParts, AND_OPER);
    }

    /**
     * Converts a range constraint into a predicate to use in an Elasticsearch query.
     * @param fieldName The name of the field for the given ValueSet constraint.
     * @param constraint The constraint to apply to the given field.
     * @return A string describing the constraint for pushing down into Elasticsearch.
     */
    protected static String getPredicateFromRange(String fieldName, ValueSet constraint)
    {
        logger.info("getPredicateFromRange - enter: " + fieldName);

        List<String> singleValues = new ArrayList<>();
        List<String> disjuncts = new ArrayList<>();
        for (Range range : constraint.getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue().toString());
            }
            else {
                String rangeConjuncts = "(";
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case EXACTLY:
                            rangeConjuncts += ">=" + range.getLow().getValue().toString();
                            break;
                        case ABOVE:
                            rangeConjuncts += ">" + range.getLow().getValue().toString();
                            break;
                        case BELOW:
                            logger.warn("Low Marker should never use BELOW bound: " + range);
                            continue;
                        default:
                            logger.warn("Unhandled bound: " + range.getLow().getBound());
                            continue;
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY:
                            rangeConjuncts += AND_OPER + "<=" + range.getHigh().getValue().toString();
                            break;
                        case BELOW:
                            rangeConjuncts += AND_OPER + "<" + range.getHigh().getValue().toString();
                            break;
                        case ABOVE:
                            logger.warn("High Marker should never use ABOVE bound: " + range);
                            continue;
                        default:
                            logger.warn("Unhandled bound: " + range.getHigh().getBound());
                            continue;
                    }
                }
                disjuncts.add(rangeConjuncts + ")");
            }
        }

        if (!singleValues.isEmpty()) {
            // value1 OR value2 OR value3...
            disjuncts.add(Strings.collectionToDelimitedString(singleValues, OR_OPER));
        }

        if (disjuncts.isEmpty()) {
            // There are no ranges stored.
            return EMPTY_PREDICATE;
        }

        // field:((>=value1 AND <=value2) OR value3 OR value4 OR value5...)
        return fieldName + ":(" + Strings.collectionToDelimitedString(disjuncts, OR_OPER) + ")";
    }
}
