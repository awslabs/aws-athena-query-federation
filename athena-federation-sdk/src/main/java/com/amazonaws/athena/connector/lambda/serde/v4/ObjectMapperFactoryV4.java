/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.PingRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.PingResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.AllOrNoneValueSetSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ArrowTypeSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.EncryptionKeySerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.EquatableValueSetSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetSplitsRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetSplitsResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetTableLayoutRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetTableLayoutResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetTableRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.GetTableResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.LambdaFunctionExceptionSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ListSchemasRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ListSchemasResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ListTablesRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ListTablesResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.MarkerSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.RangeSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ReadRecordsRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ReadRecordsResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.RemoteReadRecordsResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.S3SpillLocationSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.SortedRangeSetSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.SpillLocationSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.SplitSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.TableNameSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.UserDefinedFunctionRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.UserDefinedFunctionResponseSerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.ValueSetSerDe;
import com.amazonaws.services.lambda.invoke.LambdaFunctionException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.DeserializerFactoryConfig;
import com.fasterxml.jackson.databind.cfg.SerializerFactoryConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.deser.DeserializerFactory;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;

public class ObjectMapperFactoryV4
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final String LAMDA_EXCEPTION_CLASS_NAME = LambdaFunctionException.class.getName();

    private static final SerializerFactory SERIALIZER_FACTORY;

    static {
        // Serializers can be static since they don't need a BlockAllocator
        ImmutableList<JsonSerializer<?>> sers = ImmutableList.of(createRequestSerializer(), createResponseSerializer());
        SimpleSerializers serializers = new SimpleSerializers(sers);
        SerializerFactoryConfig config = new SerializerFactoryConfig().withAdditionalSerializers(serializers);
        SERIALIZER_FACTORY = new StrictSerializerFactory(config);
    }

    private ObjectMapperFactoryV4() {}

    /**
     * Custom SerializerFactory that *only* uses the custom serializers that we inject into the {@link ObjectMapper}.
     */
    private static class StrictSerializerFactory extends BeanSerializerFactory
    {
        private StrictSerializerFactory(SerializerFactoryConfig config)
        {
            super(config);
        }

        @Override
        public StrictSerializerFactory withConfig(SerializerFactoryConfig config)
        {
            if (_factoryConfig == config) {
                return this;
            }
            return new StrictSerializerFactory(config);
        }

        @Override
        @SuppressWarnings("unchecked")
        public JsonSerializer<Object> createSerializer(SerializerProvider prov, JavaType origType)
                throws JsonMappingException
        {
            for (Serializers serializers : customSerializers()) {
                JsonSerializer<?> ser = serializers.findSerializer(prov.getConfig(), origType, null);
                if (ser != null) {
                    return (JsonSerializer<Object>) ser;
                }
            }
            throw new IllegalArgumentException("No explicitly configured serializer for " + origType);
        }
    }

    /**
     * Custom DeserializerFactory that *only* uses the custom deserializers that we inject into the {@link ObjectMapper}.
     */
    private static class StrictDeserializerFactory extends BeanDeserializerFactory
    {
        private StrictDeserializerFactory(DeserializerFactoryConfig config)
        {
            super(config);
        }

        @Override
        public DeserializerFactory withConfig(DeserializerFactoryConfig config)
        {
            if (_factoryConfig == config) {
                return this;
            }
            return new StrictDeserializerFactory(config);
        }

        @Override
        @SuppressWarnings("unchecked")
        public JsonDeserializer<Object> createBeanDeserializer(DeserializationContext ctxt, JavaType type, BeanDescription beanDesc)
                throws JsonMappingException
        {
            for (Deserializers d  : _factoryConfig.deserializers()) {
                JsonDeserializer<?> deser = d.findBeanDeserializer(type, ctxt.getConfig(), beanDesc);
                if (deser != null) {
                    return (JsonDeserializer<Object>) deser;
                }
            }
            throw new IllegalArgumentException("No explicitly configured deserializer for " + type);
        }
    }

    /**
     * Locked down ObjectMapper that only uses the serializers/deserializers provided and does not fall back to annotation or reflection
     * based serialization.
     */
    private static class StrictObjectMapper extends ObjectMapper
    {
        private StrictObjectMapper(BlockAllocator allocator)
        {
            super(JSON_FACTORY);
            _serializerFactory = SERIALIZER_FACTORY;

            ImmutableMap<Class<?>, JsonDeserializer<?>> desers = ImmutableMap.of(
                    FederationRequest.class, createRequestDeserializer(allocator),
                    FederationResponse.class, createResponseDeserializer(allocator),
                    LambdaFunctionException.class, new LambdaFunctionExceptionSerDe.Deserializer());
            SimpleDeserializers deserializers = new SimpleDeserializers(desers);
            DeserializerFactoryConfig dConfig = new DeserializerFactoryConfig().withAdditionalDeserializers(deserializers);
            _deserializationContext = new DefaultDeserializationContext.Impl(new StrictDeserializerFactory(dConfig));
            // required by LambdaInvokerFactory
            disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        }
    }

    public static ObjectMapper create(BlockAllocator allocator)
    {
        return new StrictObjectMapper(allocator);
    }

    private static FederationRequestSerDeV4.Serializer createRequestSerializer()
    {
        FederatedIdentitySerDe.Serializer identity = new FederatedIdentitySerDe.Serializer();
        TableNameSerDe.Serializer tableName = new TableNameSerDe.Serializer();
        VersionedSerDe.Serializer<Schema> schema = new SchemaSerDeV4.Serializer();
        VersionedSerDe.Serializer<Block> block = new BlockSerDeV4.Serializer(schema);
        ArrowTypeSerDe.Serializer arrowType = new ArrowTypeSerDe.Serializer();
        MarkerSerDe.Serializer marker = new MarkerSerDe.Serializer(block);
        RangeSerDe.Serializer range = new RangeSerDe.Serializer(marker);
        EquatableValueSetSerDe.Serializer equatableValueSet = new EquatableValueSetSerDe.Serializer(block);
        SortedRangeSetSerDe.Serializer sortedRangeSet = new SortedRangeSetSerDe.Serializer(arrowType, range);
        AllOrNoneValueSetSerDe.Serializer allOrNoneValueSet = new AllOrNoneValueSetSerDe.Serializer(arrowType);
        ValueSetSerDe.Serializer valueSet = new ValueSetSerDe.Serializer(equatableValueSet, sortedRangeSet, allOrNoneValueSet);
        VersionedSerDe.Serializer<FunctionName> functionName = new FunctionNameSerDeV4.Serializer();
        ConstantExpressionSerDeV4.Serializer constantExpression = new ConstantExpressionSerDeV4.Serializer(block, arrowType);
        FunctionCallExpressionSerDeV4.Serializer functionCallExpression = new FunctionCallExpressionSerDeV4.Serializer(functionName, arrowType);
        VariableExpressionSerDeV4.Serializer variableExpression = new VariableExpressionSerDeV4.Serializer(arrowType);
        VersionedSerDe.Serializer<FederationExpression> federationExpression = new FederationExpressionSerDeV4.Serializer(constantExpression, functionCallExpression, variableExpression);
        functionCallExpression.setFederationExpressionSerializer(federationExpression);
        VersionedSerDe.Serializer<OrderByField> orderByField = new OrderByFieldSerDeV4.Serializer();
        VersionedSerDe.Serializer<Constraints> constraints = new ConstraintsSerDeV4.Serializer(valueSet, federationExpression, orderByField);
        S3SpillLocationSerDe.Serializer s3SpillLocation = new S3SpillLocationSerDe.Serializer();
        SpillLocationSerDe.Serializer spillLocation = new SpillLocationSerDe.Serializer(s3SpillLocation);
        EncryptionKeySerDe.Serializer encryptionKey = new EncryptionKeySerDe.Serializer();
        SplitSerDe.Serializer split = new SplitSerDe.Serializer(spillLocation, encryptionKey);
        PingRequestSerDe.Serializer ping = new PingRequestSerDe.Serializer(identity);
        ListSchemasRequestSerDe.Serializer listSchemas = new ListSchemasRequestSerDe.Serializer(identity);
        ListTablesRequestSerDe.Serializer listTables = new ListTablesRequestSerDe.Serializer(identity);
        GetTableRequestSerDe.Serializer getTable = new GetTableRequestSerDe.Serializer(identity, tableName);
        GetTableLayoutRequestSerDe.Serializer getTableLayout = new GetTableLayoutRequestSerDe.Serializer(identity, tableName, constraints, schema);
        GetSplitsRequestSerDe.Serializer getSplits = new GetSplitsRequestSerDe.Serializer(identity, tableName, block, constraints);
        ReadRecordsRequestSerDe.Serializer readRecords = new ReadRecordsRequestSerDe.Serializer(identity, tableName, constraints, schema, split);
        UserDefinedFunctionRequestSerDe.Serializer userDefinedFunction = new UserDefinedFunctionRequestSerDe.Serializer(identity, block, schema);
        GetDataSourceCapabilitiesRequestSerDeV4.Serializer getDataSourceCapabilities = new GetDataSourceCapabilitiesRequestSerDeV4.Serializer(identity);
        return new FederationRequestSerDeV4.Serializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                userDefinedFunction,
                getDataSourceCapabilities);
    }

    private static FederationRequestSerDeV4.Deserializer createRequestDeserializer(BlockAllocator allocator)
    {
        FederatedIdentitySerDe.Deserializer identity = new FederatedIdentitySerDe.Deserializer();
        TableNameSerDe.Deserializer tableName = new TableNameSerDe.Deserializer();
        VersionedSerDe.Deserializer<Schema> schema = new SchemaSerDeV4.Deserializer();
        VersionedSerDe.Deserializer<Block> block = new BlockSerDeV4.Deserializer(allocator, schema);
        ArrowTypeSerDe.Deserializer arrowType = new ArrowTypeSerDe.Deserializer();
        MarkerSerDe.Deserializer marker = new MarkerSerDe.Deserializer(block);
        RangeSerDe.Deserializer range = new RangeSerDe.Deserializer(marker);
        EquatableValueSetSerDe.Deserializer equatableValueSet = new EquatableValueSetSerDe.Deserializer(block);
        SortedRangeSetSerDe.Deserializer sortedRangeSet = new SortedRangeSetSerDe.Deserializer(arrowType, range);
        AllOrNoneValueSetSerDe.Deserializer allOrNoneValueSet = new AllOrNoneValueSetSerDe.Deserializer(arrowType);
        ValueSetSerDe.Deserializer valueSet = new ValueSetSerDe.Deserializer(equatableValueSet, sortedRangeSet, allOrNoneValueSet);

        VersionedSerDe.Deserializer<FunctionName> functionName = new FunctionNameSerDeV4.Deserializer();
        ConstantExpressionSerDeV4.Deserializer constantExpression = new ConstantExpressionSerDeV4.Deserializer(block, arrowType);
        FunctionCallExpressionSerDeV4.Deserializer functionCallExpression = new FunctionCallExpressionSerDeV4.Deserializer(functionName, arrowType);
        VariableExpressionSerDeV4.Deserializer variableExpression = new VariableExpressionSerDeV4.Deserializer(arrowType);
        VersionedSerDe.Deserializer<FederationExpression> federationExpression = new FederationExpressionSerDeV4.Deserializer(constantExpression, functionCallExpression, variableExpression);
        functionCallExpression.setFederationExpressionSerializer(federationExpression);
        VersionedSerDe.Deserializer<OrderByField> orderByField = new OrderByFieldSerDeV4.Deserializer();
        VersionedSerDe.Deserializer<Constraints> constraints = new ConstraintsSerDeV4.Deserializer(valueSet, federationExpression, orderByField);

        S3SpillLocationSerDe.Deserializer s3SpillLocation = new S3SpillLocationSerDe.Deserializer();
        SpillLocationSerDe.Deserializer spillLocation = new SpillLocationSerDe.Deserializer(s3SpillLocation);
        EncryptionKeySerDe.Deserializer encryptionKey = new EncryptionKeySerDe.Deserializer();
        SplitSerDe.Deserializer split = new SplitSerDe.Deserializer(spillLocation, encryptionKey);

        PingRequestSerDe.Deserializer ping = new PingRequestSerDe.Deserializer(identity);
        ListSchemasRequestSerDe.Deserializer listSchemas = new ListSchemasRequestSerDe.Deserializer(identity);
        ListTablesRequestSerDe.Deserializer listTables = new ListTablesRequestSerDe.Deserializer(identity);
        GetTableRequestSerDe.Deserializer getTable = new GetTableRequestSerDe.Deserializer(identity, tableName);
        GetTableLayoutRequestSerDe.Deserializer getTableLayout = new GetTableLayoutRequestSerDe.Deserializer(identity, tableName, constraints, schema);
        GetSplitsRequestSerDe.Deserializer getSplits = new GetSplitsRequestSerDe.Deserializer(identity, tableName, block, constraints);
        ReadRecordsRequestSerDe.Deserializer readRecords = new ReadRecordsRequestSerDe.Deserializer(identity, tableName, constraints, schema, split);
        UserDefinedFunctionRequestSerDe.Deserializer userDefinedFunction = new UserDefinedFunctionRequestSerDe.Deserializer(identity, block, schema);
        GetDataSourceCapabilitiesRequestSerDeV4.Deserializer getDataSourceCapabilities = new GetDataSourceCapabilitiesRequestSerDeV4.Deserializer(identity);

        return new FederationRequestSerDeV4.Deserializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                userDefinedFunction,
                getDataSourceCapabilities);
    }

    private static FederationResponseSerDeV4.Serializer createResponseSerializer()
    {
        TableNameSerDe.Serializer tableName = new TableNameSerDe.Serializer();
        VersionedSerDe.Serializer<Schema> schema = new SchemaSerDeV4.Serializer();
        VersionedSerDe.Serializer<Block> block = new BlockSerDeV4.Serializer(schema);
        S3SpillLocationSerDe.Serializer s3SpillLocation = new S3SpillLocationSerDe.Serializer();
        SpillLocationSerDe.Serializer spillLocation = new SpillLocationSerDe.Serializer(s3SpillLocation);
        EncryptionKeySerDe.Serializer encryptionKey = new EncryptionKeySerDe.Serializer();
        SplitSerDe.Serializer split = new SplitSerDe.Serializer(spillLocation, encryptionKey);

        PingResponseSerDe.Serializer ping = new PingResponseSerDe.Serializer();
        ListSchemasResponseSerDe.Serializer listSchemas = new ListSchemasResponseSerDe.Serializer();
        ListTablesResponseSerDe.Serializer listTables = new ListTablesResponseSerDe.Serializer(tableName);
        GetTableResponseSerDe.Serializer getTable = new GetTableResponseSerDe.Serializer(tableName, schema);
        GetTableLayoutResponseSerDe.Serializer getTableLayout = new GetTableLayoutResponseSerDe.Serializer(tableName, block);
        GetSplitsResponseSerDe.Serializer getSplits = new GetSplitsResponseSerDe.Serializer(split);
        ReadRecordsResponseSerDe.Serializer readRecords = new ReadRecordsResponseSerDe.Serializer(block);
        RemoteReadRecordsResponseSerDe.Serializer remoteReadRecords = new RemoteReadRecordsResponseSerDe.Serializer(schema, spillLocation, encryptionKey);
        UserDefinedFunctionResponseSerDe.Serializer userDefinedFunction = new UserDefinedFunctionResponseSerDe.Serializer(block);
        VersionedSerDe.Serializer<OptimizationSubType> optimizationSubtype = new OptimizationSubTypeSerDeV4.Serializer();
        GetDataSourceCapabilitiesResponseSerDeV4.Serializer getDataSourceCapabilities = new GetDataSourceCapabilitiesResponseSerDeV4.Serializer(optimizationSubtype);

        return new FederationResponseSerDeV4.Serializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                remoteReadRecords,
                userDefinedFunction,
                getDataSourceCapabilities);
    }

    private static FederationResponseSerDeV4.Deserializer createResponseDeserializer(BlockAllocator allocator)
    {
        TableNameSerDe.Deserializer tableName = new TableNameSerDe.Deserializer();
        VersionedSerDe.Deserializer<Schema> schema = new SchemaSerDeV4.Deserializer();
        VersionedSerDe.Deserializer<Block> block = new BlockSerDeV4.Deserializer(allocator, schema);
        S3SpillLocationSerDe.Deserializer s3SpillLocation = new S3SpillLocationSerDe.Deserializer();
        SpillLocationSerDe.Deserializer spillLocation = new SpillLocationSerDe.Deserializer(s3SpillLocation);
        EncryptionKeySerDe.Deserializer encryptionKey = new EncryptionKeySerDe.Deserializer();
        SplitSerDe.Deserializer split = new SplitSerDe.Deserializer(spillLocation, encryptionKey);

        PingResponseSerDe.Deserializer ping = new PingResponseSerDe.Deserializer();
        ListSchemasResponseSerDe.Deserializer listSchemas = new ListSchemasResponseSerDe.Deserializer();
        ListTablesResponseSerDe.Deserializer listTables = new ListTablesResponseSerDe.Deserializer(tableName);
        GetTableResponseSerDe.Deserializer getTable = new GetTableResponseSerDe.Deserializer(tableName, schema);
        GetTableLayoutResponseSerDe.Deserializer getTableLayout = new GetTableLayoutResponseSerDe.Deserializer(tableName, block);
        GetSplitsResponseSerDe.Deserializer getSplits = new GetSplitsResponseSerDe.Deserializer(split);
        ReadRecordsResponseSerDe.Deserializer readRecords = new ReadRecordsResponseSerDe.Deserializer(block);
        RemoteReadRecordsResponseSerDe.Deserializer remoteReadRecords = new RemoteReadRecordsResponseSerDe.Deserializer(schema, spillLocation, encryptionKey);
        UserDefinedFunctionResponseSerDe.Deserializer userDefinedFunction = new UserDefinedFunctionResponseSerDe.Deserializer(block);
        VersionedSerDe.Deserializer<OptimizationSubType> optimizationSubtype = new OptimizationSubTypeSerDeV4.Deserializer();
        GetDataSourceCapabilitiesResponseSerDeV4.Deserializer getDataSourceCapabilities = new GetDataSourceCapabilitiesResponseSerDeV4.Deserializer(optimizationSubtype);

        return new FederationResponseSerDeV4.Deserializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                remoteReadRecords,
                userDefinedFunction,
                getDataSourceCapabilities);
    }
}
