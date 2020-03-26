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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.PingRequestSerDe;
import com.amazonaws.athena.connector.lambda.serde.PingResponseSerDe;
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

public class ObjectMapperFactoryV2
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

    private ObjectMapperFactoryV2(){}

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

    private static FederationRequestSerDe.Serializer createRequestSerializer()
    {
        FederatedIdentitySerDe.Serializer identity = new FederatedIdentitySerDe.Serializer();
        TableNameSerDe.Serializer tableName = new TableNameSerDe.Serializer();
        SchemaSerDe.Serializer schema = new SchemaSerDe.Serializer();
        BlockSerDe.Serializer block = new BlockSerDe.Serializer(schema);
        ArrowTypeSerDe.Serializer arrowType = new ArrowTypeSerDe.Serializer();
        MarkerSerDe.Serializer marker = new MarkerSerDe.Serializer(block);
        RangeSerDe.Serializer range = new RangeSerDe.Serializer(marker);
        EquatableValueSetSerDe.Serializer equatableValueSet = new EquatableValueSetSerDe.Serializer(block);
        SortedRangeSetSerDe.Serializer sortedRangeSet = new SortedRangeSetSerDe.Serializer(arrowType, range);
        AllOrNoneValueSetSerDe.Serializer allOrNoneValueSet = new AllOrNoneValueSetSerDe.Serializer(arrowType);
        ValueSetSerDe.Serializer valueSet = new ValueSetSerDe.Serializer(equatableValueSet, sortedRangeSet, allOrNoneValueSet);
        ConstraintsSerDe.Serializer constraints = new ConstraintsSerDe.Serializer(valueSet);
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
        return new FederationRequestSerDe.Serializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                userDefinedFunction);
    }

    private static FederationRequestSerDe.Deserializer createRequestDeserializer(BlockAllocator allocator)
    {
        FederatedIdentitySerDe.Deserializer identity = new FederatedIdentitySerDe.Deserializer();
        TableNameSerDe.Deserializer tableName = new TableNameSerDe.Deserializer();
        SchemaSerDe.Deserializer schema = new SchemaSerDe.Deserializer();
        BlockSerDe.Deserializer block = new BlockSerDe.Deserializer(allocator, schema);
        ArrowTypeSerDe.Deserializer arrowType = new ArrowTypeSerDe.Deserializer();
        MarkerSerDe.Deserializer marker = new MarkerSerDe.Deserializer(block);
        RangeSerDe.Deserializer range = new RangeSerDe.Deserializer(marker);
        EquatableValueSetSerDe.Deserializer equatableValueSet = new EquatableValueSetSerDe.Deserializer(block);
        SortedRangeSetSerDe.Deserializer sortedRangeSet = new SortedRangeSetSerDe.Deserializer(arrowType, range);
        AllOrNoneValueSetSerDe.Deserializer allOrNoneValueSet = new AllOrNoneValueSetSerDe.Deserializer(arrowType);
        ValueSetSerDe.Deserializer valueSet = new ValueSetSerDe.Deserializer(equatableValueSet, sortedRangeSet, allOrNoneValueSet);
        ConstraintsSerDe.Deserializer constraints = new ConstraintsSerDe.Deserializer(valueSet);
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

        return new FederationRequestSerDe.Deserializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                userDefinedFunction);
    }

    private static FederationResponseSerDe.Serializer createResponseSerializer()
    {
        TableNameSerDe.Serializer tableName = new TableNameSerDe.Serializer();
        SchemaSerDe.Serializer schema = new SchemaSerDe.Serializer();
        BlockSerDe.Serializer block = new BlockSerDe.Serializer(schema);
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

        return new FederationResponseSerDe.Serializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                remoteReadRecords,
                userDefinedFunction);
    }

    private static FederationResponseSerDe.Deserializer createResponseDeserializer(BlockAllocator allocator)
    {
        TableNameSerDe.Deserializer tableName = new TableNameSerDe.Deserializer();
        SchemaSerDe.Deserializer schema = new SchemaSerDe.Deserializer();
        BlockSerDe.Deserializer block = new BlockSerDe.Deserializer(allocator, schema);
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

        return new FederationResponseSerDe.Deserializer(
                ping,
                listSchemas,
                listTables,
                getTable,
                getTableLayout,
                getSplits,
                readRecords,
                remoteReadRecords,
                userDefinedFunction);
    }
}
