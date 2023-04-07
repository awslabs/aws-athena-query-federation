/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.protobuf;

import com.amazonaws.athena.connector.lambda.data.AthenaFederationIpcOption;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ProtobufMessageConverter 
{
    
    private static final String ALL_OR_NONE_VALUE_SET_TYPE = "@AllOrNoneValueSet";
    private static final String EQUATABLE_VALUE_SET_TYPE = "@EquatableValueSet";
    private static final String SORTED_RANGE_SET_TYPE = "@SortedRangeSet";

    private static final Logger logger = LoggerFactory.getLogger(ProtobufMessageConverter.class);

    private ProtobufMessageConverter()
    {
        // do nothing
    }

    public static com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage toProtoArrowType(ArrowType arrowType)
    {
        com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage.Builder arrowTypeMessageBuilder = com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage.newBuilder();
       
        // BaseSerializer::writeType does the same thing as here 
        arrowTypeMessageBuilder.setType(arrowType.getClass().getSimpleName());

        //now, we have to write whatever fields the ArrowType constructor would take in based on the subtype.
        // Refer to ArrowTypeSerDe and the subclasses' doTypedSerialize implementation
        // all no-arg constructors have nothing to do here, so we omit them
        if (arrowType instanceof ArrowType.FloatingPoint) {
            var fp = (ArrowType.FloatingPoint) arrowType;
            arrowTypeMessageBuilder.setPrecision(fp.getPrecision().name());
        }
        else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intObj = (ArrowType.Int) arrowType;
            arrowTypeMessageBuilder.setBitWidth(intObj.getBitWidth()).setIsSigned(intObj.getIsSigned());
        }
        else if (arrowType instanceof ArrowType.FixedSizeList) {
            ArrowType.FixedSizeList fixedSizeList = (ArrowType.FixedSizeList) arrowType;
            arrowTypeMessageBuilder.setListSize(fixedSizeList.getListSize());
        }
        else if (arrowType instanceof ArrowType.FixedSizeBinary) {
            ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary) arrowType;
            arrowTypeMessageBuilder.setByteWidth(fixedSizeBinary.getByteWidth());
        }
        else if (arrowType instanceof ArrowType.Union) {
            ArrowType.Union union = (ArrowType.Union) arrowType;
            arrowTypeMessageBuilder.setMode(com.amazonaws.athena.connector.lambda.proto.arrowtype.UnionMode.valueOf(union.getMode().name()));
            arrowTypeMessageBuilder.addAllTypeIds(Arrays.stream(union.getTypeIds()).boxed().collect(Collectors.toList()));
        }
        else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
            arrowTypeMessageBuilder.setPrecision("" + decimal.getPrecision());
            arrowTypeMessageBuilder.setScale(decimal.getScale());
        }
        else if (arrowType instanceof ArrowType.Date) {
            ArrowType.Date arrowDate = (ArrowType.Date) arrowType;
            arrowTypeMessageBuilder.setUnit(arrowDate.getUnit().name());
        }
        else if (arrowType instanceof ArrowType.Time) {
            ArrowType.Time arrowTime = (ArrowType.Time) arrowType;
            arrowTypeMessageBuilder.setUnit(arrowTime.getUnit().name());
            arrowTypeMessageBuilder.setBitWidth(arrowTime.getBitWidth());
        }
        else if (arrowType instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp arrowTimestamp = (ArrowType.Timestamp) arrowType;
            arrowTypeMessageBuilder.setUnit(arrowTimestamp.getUnit().name());
            arrowTypeMessageBuilder.setTimezone(arrowTimestamp.getTimezone());
        }
        else if (arrowType instanceof ArrowType.Interval) {
            ArrowType.Interval interval = (ArrowType.Interval) arrowType;
            arrowTypeMessageBuilder.setUnit(interval.getUnit().name());
        }

        return arrowTypeMessageBuilder.build();
    }

    // TODO: Add all supported types
    public static ArrowType fromProtoArrowType(com.amazonaws.athena.connector.lambda.proto.arrowtype.ArrowTypeMessage arrowTypeMessage)
    {
        var className = arrowTypeMessage.getType();
        
        // we can use reflection for the classes that have no-arg constructors, but 
        // there's not a good way to do it for classes with arg constructors.
        if (className.equals("FloatingPoint")) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.valueOf(arrowTypeMessage.getPrecision()));
        }
        else if (className.equals("Int")) {
            return new ArrowType.Int(arrowTypeMessage.getBitWidth(), arrowTypeMessage.getIsSigned());
        }
        else if (className.equals("Utf8")) {
            return new ArrowType.Utf8();
        }
        else if (className.equals("Null")) {
            return new ArrowType.Null();
        }
        else if (className.equals("Struct")) {
            return new ArrowType.Struct();
        }
        else if (className.equals("List")) {
            return new ArrowType.List();
        }
        else if (className.equals("Map")) {
            return new ArrowType.Map(false); // this is hard-coded in the existing SerDe to always be false.
        }
        else if (className.equals("Binary")) {
            return new ArrowType.Binary();
        }
        else if (className.equals("Bool")) {
            return new ArrowType.Bool();
        }
        else if (className.equals("FixedSizeList")) {
            return new ArrowType.FixedSizeList(arrowTypeMessage.getListSize());
        }
        else if (className.equals("FixedSizeBinary")) {
            return new ArrowType.FixedSizeBinary(arrowTypeMessage.getByteWidth());
        }
        else if (className.equals("Union")) {
            return new ArrowType.Union(UnionMode.valueOf(arrowTypeMessage.getMode().name()), arrowTypeMessage.getTypeIdsList().stream().mapToInt(Integer::intValue).toArray());
        }
        else if (className.equals("Decimal")) {
            return new ArrowType.Decimal(Integer.valueOf(arrowTypeMessage.getPrecision()), arrowTypeMessage.getScale());
        }
        else if (className.equals("Date")) {
            return new ArrowType.Date(DateUnit.valueOf(arrowTypeMessage.getUnit()));
        }
        else if (className.equals("Time")) {
            return new ArrowType.Time(TimeUnit.valueOf(arrowTypeMessage.getUnit()), arrowTypeMessage.getBitWidth());
        }
        else if (className.equals("Timestamp")) {
            return new ArrowType.Timestamp(TimeUnit.valueOf(arrowTypeMessage.getUnit()), arrowTypeMessage.getTimezone());
        }
        else if (className.equals("Interval")) {
            return new ArrowType.Interval(IntervalUnit.valueOf(arrowTypeMessage.getUnit()));
        }

        return null;
    }

    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker toProtoMarker(Marker marker)
    {
        return com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker.newBuilder()
            .setValueBlock(ProtobufMessageConverter.toProtoBlock(marker.getValueBlock()))
            .setBound(com.amazonaws.athena.connector.lambda.proto.domain.predicate.Bound.valueOf(marker.getBound().name()))
            .setNullValue(marker.isNullValue())
            .build();
    }

    public static Marker fromProtoMarker(BlockAllocator blockAllocator, com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker protoMarker)
    {
        return new Marker(
            ProtobufMessageConverter.fromProtoBlock(blockAllocator, protoMarker.getValueBlock()),
            Marker.Bound.valueOf(protoMarker.getBound().name()),
            protoMarker.getNullValue()
        );
    }

    public static List<com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range> toProtoRanges(List<Range> ranges)
    {
        return ranges.stream()
            .map(range -> com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range.newBuilder()
                    .setLow(toProtoMarker(range.getLow()))
                    .setHigh(toProtoMarker(range.getHigh()))
                    .build())
            .collect(Collectors.toList());
    }

    public static List<Range> fromProtoRanges(BlockAllocator blockAllocator, List<com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range> protoRanges)
    {
        return protoRanges.stream()
            .map(range -> new Range(
                fromProtoMarker(blockAllocator, range.getLow()),
                fromProtoMarker(blockAllocator, range.getHigh())
            ))
            .collect(Collectors.toList());
    }

    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet toProtoValueSet(ValueSet valueSet)
    {
        com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet.Builder outBuilder =
             com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet.newBuilder();

        if (valueSet instanceof AllOrNoneValueSet) {
            AllOrNoneValueSet allOrNone = (AllOrNoneValueSet) valueSet;
            return outBuilder
                .setTypeField(ALL_OR_NONE_VALUE_SET_TYPE)
                .setArrowTypeMessage(ProtobufMessageConverter.toProtoArrowType(allOrNone.getType()))
                .setAll(allOrNone.isAll())
                .setNullAllowed(allOrNone.isNullAllowed())
                .build();
        } 
        else if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet eqValueSet = (EquatableValueSet) valueSet;
            return outBuilder
                .setTypeField(EQUATABLE_VALUE_SET_TYPE)
                .setValueBlock(ProtobufMessageConverter.toProtoBlock(eqValueSet.getValueBlock()))
                .setWhiteList(eqValueSet.isWhiteList())
                .setNullAllowed(eqValueSet.isNullAllowed())
                .build();
        } 
        else if (valueSet instanceof SortedRangeSet) {
            SortedRangeSet sortedRangeSet = (SortedRangeSet) valueSet;
            return outBuilder
                .setTypeField(SORTED_RANGE_SET_TYPE)
                .setArrowTypeMessage(ProtobufMessageConverter.toProtoArrowType(sortedRangeSet.getType()))
                .addAllRanges(toProtoRanges(sortedRangeSet.getOrderedRanges()))
                .setNullAllowed(sortedRangeSet.isNullAllowed())
                .build();
        }
        else {
            throw new IllegalArgumentException("Unexpected subtype of ValueSet encountered.");
        }
    }

    public static ValueSet fromProtoValueSet(BlockAllocator blockAllocator, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet protoValueSet)
    {
        String type = protoValueSet.getTypeField();
        if (type.equals(ALL_OR_NONE_VALUE_SET_TYPE)) {
            return new AllOrNoneValueSet(
                fromProtoArrowType(protoValueSet.getArrowTypeMessage()),
                protoValueSet.getAll(),
                protoValueSet.getNullAllowed()
            );
        }
        else if (type.equals(EQUATABLE_VALUE_SET_TYPE)) {
            return new EquatableValueSet(
                fromProtoBlock(blockAllocator, protoValueSet.getValueBlock()),
                protoValueSet.getWhiteList(),
                protoValueSet.getNullAllowed()
            );
        }
        else if (type.equals(SORTED_RANGE_SET_TYPE)) {
            // this static copy is how the deserializer builds the Sorted Range Set as well
            return SortedRangeSet.copyOf(
                fromProtoArrowType(protoValueSet.getArrowTypeMessage()),
                fromProtoRanges(blockAllocator, protoValueSet.getRangesList()),
                protoValueSet.getNullAllowed()
            );
        }
        else {
            throw new IllegalArgumentException("Unexpected value set received over the wire - type " + type + " is not recognized.");
        }
    }

    public static Map<String, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet> toProtoSummary(Map<String, ValueSet> summaryMap)
    {
        return summaryMap.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> toProtoValueSet(e.getValue())
            ));
    }

    public static Map<String, ValueSet> fromProtoSummary(BlockAllocator blockAllocator, Map<String, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet> protoSummaryMap)
    {
        return protoSummaryMap.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> fromProtoValueSet(blockAllocator, e.getValue())
            ));
    }

    public static com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints toProtoConstraints(Constraints constraints)
    {
        return com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints.newBuilder()
            .putAllSummary(toProtoSummary(constraints.getSummary()))
            .build();
    }

    public static Constraints fromProtoConstraints(BlockAllocator blockAllocator, com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints protoConstraints)
    {
        return new Constraints(fromProtoSummary(blockAllocator, protoConstraints.getSummaryMap()));
    }

    // WARNING - This logic is compliant with the existing ObjectMapperV2 implementation. In ObjectMapperV3, the logic changed.
    // Therefore, to maintain compliance, we MUST either 1) return this (and the corresopnding From method) conditionally based on the version, or
    // 2) wait until we officially swap versions so we can just use the latest.
    public static ByteString toProtoSchemaBytes(Schema schema)
    {
        ByteArrayOutputStream outSchema = new ByteArrayOutputStream();
        // TODO: Rebase and pull in the static ipc option added recently to master
        IpcOption option = new IpcOption(true, MetadataVersion.V4);
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outSchema)), schema, option);
        }
        catch (IOException ie) {
            // for now, ignore
        }
        return ByteString.copyFrom(outSchema.toByteArray());
    }
    
    public static com.amazonaws.athena.connector.lambda.proto.data.Block toProtoBlock(Block block)
    {
        ByteArrayOutputStream outRecords = new ByteArrayOutputStream();
        try (var batch = block.getRecordBatch()) {
            if (batch.getLength() > 0) { // if we don't do this conditionally, it writes a non-empty string, which breaks the Jackson serdes
                // WARNING - this is also conditional on the serde version.
                serializeRecordBatch(outRecords, batch);
            }
        }
        catch (IOException ie) {
            logger.error("IO EXCEPTION CAUGHT. {}", ie);
            throw new RuntimeException(ie);
        }
        return com.amazonaws.athena.connector.lambda.proto.data.Block.newBuilder()
            .setAId(block.getAllocatorId())
            .setSchema(toProtoSchemaBytes(block.getSchema()))
            .setRecords(ByteString.copyFrom(outRecords.toByteArray()))
            .build();
    }

    public static Schema fromProtoSchema(BlockAllocator allocator, ByteString protoSchemaBytes)
    {
        ByteArrayInputStream in = new ByteArrayInputStream(protoSchemaBytes.toByteArray());
        Schema schema = null;
        try {
            schema = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)));
        }
        catch (IOException ie) {
            // ignore for now
        }
        return schema;
    }

    public static Block fromProtoBlock(BlockAllocator allocator, com.amazonaws.athena.connector.lambda.proto.data.Block protoBlock)
    {
        Schema schema = fromProtoSchema(allocator, protoBlock.getSchema());
        var block = allocator.createBlock(schema);

        var protoRecords = protoBlock.getRecords().toByteArray();
        if (protoRecords.length > 0) {
            block.loadRecordBatch(deserializeRecordBatch(allocator, protoRecords));
        }

        return block;
    }

    // preserving functionality of v2 RecordBatch SerDe, as it's the only thing we need still from Jackson
    // method is a copy of RecordBatchSerDe.java's serializer
    public static void serializeRecordBatch(OutputStream outRecords, ArrowRecordBatch batch) throws IOException
    {
        try {
            IpcOption option = AthenaFederationIpcOption.DEFAULT;
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outRecords)), batch, option);
        }
        finally {
            batch.close();
        }
    }
    

    /**
     * Method is a copy of BlockSerDe.java's deserializer for record batches.
     */
    public static ArrowRecordBatch deserializeRecordBatch(BlockAllocator allocator, byte[] protoRecords)
    {
        // WARNING - this too is dependent on what serde version we are doing. Needs to be made dynamic based on the serde version!
        AtomicReference<ArrowRecordBatch> batch = new AtomicReference<>();
            try {
                return allocator.registerBatch((BufferAllocator root) -> {
                    batch.set((ArrowRecordBatch) MessageSerializer.deserializeMessageBatch(
                            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(protoRecords))), root));
                    return batch.get();
                });
            }
            catch (Exception ex) {
                if (batch.get() != null) {
                    batch.get().close();
                }
                throw ex;
            }
    }
}
