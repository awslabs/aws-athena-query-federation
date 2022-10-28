package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateDayWriter;
import org.apache.arrow.vector.complex.writer.DateMilliWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliTZWriter;
import org.apache.arrow.vector.complex.writer.TinyIntWriter;
import org.apache.arrow.vector.complex.writer.UInt1Writer;
import org.apache.arrow.vector.complex.writer.UInt2Writer;
import org.apache.arrow.vector.complex.writer.UInt4Writer;
import org.apache.arrow.vector.complex.writer.UInt8Writer;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.codec.Charsets;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This utility class abstracts many facets of reading and writing values into Apache Arrow's FieldReader and FieldVector
 * objects.
 *
 * @note This class encourages a row wise approach to writing results. These interfaces are often viewed as simpler than
 * the would be columnar equivalents. Even though many of the systems that we've integrated with using this SDK do not
 * themselves support columnar access patterns, there is value in offering a a variant of these mechanisms that provide
 * the skeleton for columnar writing/reading of results.
 * <p>
 * The current SDK version takes the approach that experts can drop into 'native' Apache Arrow mode and simply not use
 * this abstraction. This approach of making common things easy and still enabling access to a 'power user' mode is
 * one we'd like to stick with but we'd also like to make it easier for customers that can/want a more columnar
 * experience/performance to be able to do so more easily.
 * <p>
 * In general the abstractions provided by this utility class also come with a performance hit when compared with native,
 * columnar, Apache Arrow access patterns. The performance overhead primarily results from Object overhead related to boxing
 * and/or type conversion. The second source of overhead is the constant lookup and branching of field types, vectors, readers,
 * etc.. Some of this second category of overhead can be mitigated by being mindful of how you use this class but a more
 * ideal solution would be to offer an interface that steers you in a better direction.
 * <p>
 * An issue has been opened to track the creation of a columnar variant of this utility:
 * https://github.com/awslabs/aws-athena-query-federation/issues/1
 */
public class BlockUtils
{
    public static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    /**
     * Creates a new Block with a single column and populated with the provided values.
     *
     * @param allocator The BlockAllocator to use when creating the Block.
     * @param columnName The name of the single column in the Block's Schema.
     * @param type The Apache Arrow Type of the column.
     * @param values The values to write to the new Block. Each value will be its own row.
     * @return The newly created Block with a single column Schema at populated with the provided values.
     */
    public static Block newBlock(BlockAllocator allocator, String columnName, ArrowType type, Object... values)
    {
        return newBlock(allocator, columnName, type, Arrays.asList(values));
    }

    /**
     * Creates a new Block with a single column and populated with the provided values.
     *
     * @param allocator The BlockAllocator to use when creating the Block.
     * @param columnName The name of the single column in the Block's Schema.
     * @param type The Apache Arrow Type of the column.
     * @param values The values to write to the new Block. Each value will be its own row.
     * @return The newly created Block with a single column Schema at populated with the provided values.
     */
    public static Block newBlock(BlockAllocator allocator, String columnName, ArrowType type, Collection<Object> values)
    {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addField(columnName, type);
        Schema schema = schemaBuilder.build();
        Block block = allocator.createBlock(schema);
        int count = 0;
        for (Object next : values) {
            try {
                setValue(block.getFieldVector(columnName), count++, next);
            }
            catch (Exception ex) {
                throw new RuntimeException("Error for " + type + " " + columnName + " " + next, ex);
            }
        }
        block.setRowCount(count);
        return block;
    }

    /**
     * Creates a new, empty, Block with a single column.
     *
     * @param allocator The BlockAllocator to use when creating the Block.
     * @param columnName The name of the single column in the Block's Schema.
     * @param type The Apache Arrow Type of the column.
     * @return The newly created, empty, Block with a single column Schema.
     */
    public static Block newEmptyBlock(BlockAllocator allocator, String columnName, ArrowType type)
    {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addField(columnName, type);
        Schema schema = schemaBuilder.build();
        return allocator.createBlock(schema);
    }

    /**
     * Used to set complex values (Struct, List, etc...) on the provided FieldVector.
     *
     * @param vector The FieldVector into which we should write the provided value.
     * @param pos The row number that the value should be written to.
     * @param resolver The FieldResolver that can be used to map your value to the complex type (mostly for Structs, Maps).
     * @param value The value to write.
     * @note This method incurs more Object overhead (heap churn) than using Arrow's native interface. Users of this Utility
     * should weigh their performance needs vs. the readability / ease of use.
     */
    public static void setComplexValue(FieldVector vector, int pos, FieldResolver resolver, Object value)
    {
        if (vector instanceof MapVector) {
            FieldWriter writer = ((MapVector) vector).getWriter();
            writer.setPosition(pos);
            writeMap(vector.getAllocator(),
                    writer,
                    vector.getField(),
                    pos,
                    value,
                    resolver);
        }
        else if (vector instanceof ListVector) {
            FieldWriter writer = ((ListVector) vector).getWriter();
            writer.setPosition(pos);
            writeList(vector.getAllocator(),
                    writer,
                    vector.getField(),
                    pos,
                    ((List) value),
                    resolver);
        }
        else if (vector instanceof StructVector) {
            FieldWriter writer = ((StructVector) vector).getWriter();
            writer.setPosition(pos);
            writeStruct(vector.getAllocator(),
                    writer,
                    vector.getField(),
                    pos,
                    value,
                    resolver);
        }
        else {
            throw new RuntimeException("Unsupported 'Complex' vector " +
                    vector.getClass().getSimpleName() + " for field " + vector.getField().getName());
        }
    }

    /**
     * Used to set values (Int, BigInt, Bit, etc...) on the provided FieldVector.
     *
     * @param vector The FieldVector into which we should write the provided value.
     * @param pos The row number that the value should be written to.
     * @param value The value to write.
     * @note This method incurs more Object overhead (heap churn) than using Arrow's native interface. Users of this Utility
     * should weigh their performance needs vs. the readability / ease of use.
     */
    public static void setValue(FieldVector vector, int pos, Object value)
    {
        try {
            if (value == null) {
                setNullValue(vector, pos);
                return;
            }
            /**
             * We will convert any types that are not supported by setValue to types that are supported
             * ex) (not supported) org.joda.time.LocalDateTime which is returned on read from vectors
             * will be converted to (supported) java.time.ZonedDateTime
             */

            //TODO: add all types
            switch (vector.getMinorType()) {
                case TIMESTAMPMILLITZ:
                    if (value instanceof org.joda.time.LocalDateTime) {
                        DateTimeZone dtz = ((org.joda.time.LocalDateTime) value).getChronology().getZone();
                        long dateTimeWithZone = ((org.joda.time.LocalDateTime) value).toDateTime(dtz).getMillis();
                        ((TimeStampMilliTZVector) vector).setSafe(pos, dateTimeWithZone);
                    }
                    if (value instanceof ZonedDateTime) {
                        long dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime) value);
                        ((TimeStampMilliTZVector) vector).setSafe(pos, dateTimeWithZone);
                    }
                    else if (value instanceof LocalDateTime) {
                        long dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(
                                ((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                        ((TimeStampMilliTZVector) vector).setSafe(pos, dateTimeWithZone);
                    }
                    else if (value instanceof Date) {
                        long ldtInLong = Instant.ofEpochMilli(((Date) value).getTime())
                                .atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                        long dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                        ((TimeStampMilliTZVector) vector).setSafe(pos, dateTimeWithZone);
                    }
                    else {
                        ((TimeStampMilliTZVector) vector).setSafe(pos, (long) value);
                    }
                    break;
                case DATEMILLI:
                    if (value instanceof Date) {
                        ((DateMilliVector) vector).setSafe(pos, ((Date) value).getTime());
                    }
                    else if (value instanceof LocalDateTime) {
                        ((DateMilliVector) vector).setSafe(
                                pos,
                                ((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli());
                    }
                    else {
                        ((DateMilliVector) vector).setSafe(pos, (long) value);
                    }
                    break;
                case DATEDAY:
                    if (value instanceof Date) {
                        org.joda.time.Days days = org.joda.time.Days.daysBetween(EPOCH,
                                new org.joda.time.DateTime(((Date) value).getTime()));
                        ((DateDayVector) vector).setSafe(pos, days.getDays());
                    }
                    else if (value instanceof LocalDate) {
                        int days = (int) ((LocalDate) value).toEpochDay();
                        ((DateDayVector) vector).setSafe(pos, days);
                    }
                    else if (value instanceof Long) {
                        ((DateDayVector) vector).setSafe(pos, ((Long) value).intValue());
                    }
                    else {
                        ((DateDayVector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case FLOAT8:
                    ((Float8Vector) vector).setSafe(pos, (double) value);
                    break;
                case FLOAT4:
                    ((Float4Vector) vector).setSafe(pos, (float) value);
                    break;
                case INT:
                    if (value != null && value instanceof Long) {
                        //This may seem odd at first but many frameworks (like Presto) use long as the preferred
                        //native java type for representing integers. We do this to keep type conversions simple.
                        ((IntVector) vector).setSafe(pos, ((Long) value).intValue());
                    }
                    else {
                        ((IntVector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case TINYINT:
                    if (value instanceof Byte) {
                        ((TinyIntVector) vector).setSafe(pos, (byte) value);
                    }
                    else {
                        ((TinyIntVector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case SMALLINT:
                    if (value instanceof Short) {
                        ((SmallIntVector) vector).setSafe(pos, (short) value);
                    }
                    else {
                        ((SmallIntVector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case UINT1:
                    if (value instanceof Byte) {
                        ((UInt1Vector) vector).setSafe(pos, (byte) value);
                    }
                    else {
                        ((UInt1Vector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case UINT2:
                    if (value instanceof Character) {
                        ((UInt2Vector) vector).setSafe(pos, (char) value);
                    }
                    else {
                        ((UInt2Vector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case UINT4:
                    ((UInt4Vector) vector).setSafe(pos, (int) value);
                    break;
                case UINT8:
                    if (value instanceof Long) {
                        ((UInt8Vector) vector).setSafe(pos, (long) value);
                    }
                    else {
                        ((UInt8Vector) vector).setSafe(pos, (int) value);
                    }
                    break;
                case BIGINT:
                    ((BigIntVector) vector).setSafe(pos, (long) value);
                    break;
                case VARBINARY:
                    ((VarBinaryVector) vector).setSafe(pos, (byte[]) value);
                    break;
                case DECIMAL:
                    DecimalVector dVector = ((DecimalVector) vector);
                    if (value instanceof Double) {
                        BigDecimal bdVal = new BigDecimal((double) value);
                        bdVal = bdVal.setScale(dVector.getScale(), RoundingMode.HALF_UP);
                        dVector.setSafe(pos, bdVal);
                    }
                    else {
                        BigDecimal scaledValue = ((BigDecimal) value).setScale(dVector.getScale(), RoundingMode.HALF_UP);
                        ((DecimalVector) vector).setSafe(pos, scaledValue);
                    }
                    break;
                case VARCHAR:
                    if (value instanceof Text) {
                        ((VarCharVector) vector).setSafe(pos, (Text) value);
                    }
                    else {
                        // always fall back to the object's toString()
                        ((VarCharVector) vector).setSafe(pos, value.toString().getBytes(Charsets.UTF_8));
                    }
                    break;
                case BIT:
                    if (value instanceof Integer && (int) value > 0) {
                        ((BitVector) vector).setSafe(pos, 1);
                    }
                    else if (value instanceof Boolean && (boolean) value) {
                        ((BitVector) vector).setSafe(pos, 1);
                    }
                    else {
                        ((BitVector) vector).setSafe(pos, 0);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + vector.getMinorType());
            }
        }
        catch (RuntimeException ex) {
            String fieldName = (vector != null) ? vector.getField().getName() : "null_vector";
            throw new RuntimeException("Unable to set value for field " + fieldName
                + " using value " + value
                + " of type " + vector.getMinorType(), ex);
        }
    }

    /**
     * Used to convert a specific row in the provided Block to a human readable string. This is useful for diagnostic
     * logging.
     *
     * @param block The Block to read the row from.
     * @param row The row number to read.
     * @return The human readable String representation of the requested row.
     */
    public static String rowToString(Block block, int row)
    {
        if (row > block.getRowCount()) {
            throw new IllegalArgumentException(row + " exceeds available rows " + block.getRowCount());
        }

        StringBuilder sb = new StringBuilder();
        for (FieldReader nextReader : block.getFieldReaders()) {
            try {
                nextReader.setPosition(row);
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append("[");
                sb.append(nextReader.getField().getName());
                sb.append(" : ");
                sb.append(fieldToString(nextReader));
                sb.append("]");
            }
            catch (RuntimeException ex) {
                throw new RuntimeException("Error processing field " + nextReader.getField().getName(), ex);
            }
        }

        return sb.toString();
    }

    /**
     * Used to convert a single cell for the given FieldReader to a human readable string.
     *
     * @param reader The FieldReader from which we should read the current cell. This means the position to be read should
     * have been set on the reader before calling this method.
     * @return The human readable String representation of the value at the FieldReaders current position.
     */
    public static String fieldToString(FieldReader reader)
    {
        switch (reader.getMinorType()) {
            case DATEDAY:
                return String.valueOf(reader.readInteger());
            case TIMESTAMPMILLITZ:
                return String.valueOf(DateTimeFormatterUtil.constructZonedDateTime(reader.readLong()));
            case DATEMILLI:
                return String.valueOf(reader.readLocalDateTime());
            case FLOAT8:
            case FLOAT4:
            case UINT4:
            case UINT8:
            case INT:
            case BIGINT:
            case VARCHAR:
            case BIT:
                return String.valueOf(reader.readObject());
            case DECIMAL:
                return String.valueOf(reader.readBigDecimal());
            case SMALLINT:
                return String.valueOf(reader.readShort());
            case TINYINT:
            case UINT1:
                return Integer.valueOf(reader.readByte()).toString();
            case UINT2:
                return Integer.valueOf(reader.readCharacter()).toString();
            case VARBINARY:
                return bytesToHex(reader.readByteArray());
            case STRUCT:
                StringBuilder sb = new StringBuilder();
                sb.append("{");
                for (Field child : reader.getField().getChildren()) {
                    if (sb.length() > 3) {
                        sb.append(",");
                    }
                    sb.append("[");
                    sb.append(child.getName());
                    sb.append(" : ");
                    sb.append(fieldToString(reader.reader(child.getName())));
                    sb.append("]");
                }
                sb.append("}");
                return sb.toString();
            case LIST:
                StringBuilder sbList = new StringBuilder();
                sbList.append("{");
                while (reader.next()) {
                    if (sbList.length() > 1) {
                        sbList.append(",");
                    }
                    sbList.append(fieldToString(reader.reader()));
                }
                sbList.append("}");
                return sbList.toString();
            case MAP:
                StringBuilder sbMap = new StringBuilder();
                while (reader.next()) {
                    sbMap.append(fieldToString(reader.reader()));
                }
                return sbMap.toString();
            default:
                Object obj = reader.readObject();
                return reader.getMinorType() + " - " + ((obj != null) ? obj.getClass().toString() : "null") +
                        "[ " + String.valueOf(obj) + " ]";
        }
    }

    /**
     * Copies a inclusive range of rows from one block to another.
     *
     * @param srcBlock The source Block to copy the range of rows from.
     * @param dstBlock The destination Block to copy the range of rows to.
     * @param firstRow The first row we'd like to copy.
     * @param lastRow The last row we'd like to copy.
     * @return The number of rows that were copied.
     */
    public static int copyRows(Block srcBlock, Block dstBlock, int firstRow, int lastRow)
    {
        if (firstRow > lastRow || lastRow > srcBlock.getRowCount() - 1) {
            throw new RuntimeException("src has " + srcBlock.getRowCount()
                    + " but requested copy of " + firstRow + " to " + lastRow);
        }

        for (FieldReader src : srcBlock.getFieldReaders()) {
            int dstOffset = dstBlock.getRowCount();
            for (int i = firstRow; i <= lastRow; i++) {
                FieldVector dst = dstBlock.getFieldVector(src.getField().getName());
                src.setPosition(i);
                setValue(dst, dstOffset++, src.readObject());
            }
        }

        int rowsCopied = 1 + (lastRow - firstRow);
        dstBlock.setRowCount(dstBlock.getRowCount() + rowsCopied);
        return rowsCopied;
    }

    /**
     * Checks if a row is null by checking that all fields in that row are null (aka not set).
     *
     * @param block The Block we'd like to check.
     * @param row The row number we'd like to check.
     * @return True if the entire row is null (aka all fields null/unset), False if any field has a non-null value.
     */
    public static boolean isNullRow(Block block, int row)
    {
        if (row > block.getRowCount() - 1) {
            throw new RuntimeException("block has " + block.getRowCount()
                    + " rows but requested to check " + row);
        }

        //If any column is non-null then return false
        for (FieldReader src : block.getFieldReaders()) {
            src.setPosition(row);
            if (src.isSet()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Used to write a List value.
     *
     * @param allocator The BlockAllocator which can be used to generate Apache Arrow Buffers for types
     * which require conversion to an Arrow Buffer before they can be written using the FieldWriter.
     * @param writer The FieldWriter for the List field we'd like to write into.
     * @param field The Schema details of the List Field we are writing into.
     * @param pos The position (row) in the Apache Arrow batch we are writing to.
     * @param value An iterator to the collection of values we want to write into the row.
     * @param resolver The field resolver that can be used to extract individual values from the value iterator.
     */
    @VisibleForTesting
    protected static void writeList(BufferAllocator allocator,
            FieldWriter writer,
            Field field,
            int pos,
            Iterable value,
            FieldResolver resolver)
    {
        if (value == null) {
            writer.writeNull();
            return;
        }

        //Apache Arrow List types have a single 'special' child field which gives us the concrete type of the values
        //stored in the list.
        Field child = null;
        if (field.getChildren() != null && !field.getChildren().isEmpty()) {
            child = field.getChildren().get(0);
        }

        //Mark the beginning of the list, this is essentially how Apache Arrow handles the variable length nature
        //of lists.
        writer.startList();

        Iterator itr = value.iterator();
        while (itr.hasNext()) {
            //For each item in the iterator, attempt to write it to the list.
            Object val = itr.next();
            writeAllValue(writer, child, allocator, pos, resolver, val, false);
        }
        writer.endList();
    }

    /**
     * Used to write a Struct value.
     *
     * @param allocator The BlockAllocator which can be used to generate Apache Arrow Buffers for types
     * which require conversion to an Arrow Buffer before they can be written using the FieldWriter.
     * @param writer The FieldWriter for the Struct field we'd like to write into.
     * @param field The Schema details of the Struct Field we are writing into.
     * @param pos The position (row) in the Apache Arrow batch we are writing to.
     * @param value The value we'd like to write as a struct.
     * @param resolver The field resolver that can be used to extract individual Struct fields from the value.
     */
    @VisibleForTesting
    protected static void writeStruct(BufferAllocator allocator,
            StructWriter writer,
            Field field,
            int pos,
            Object value,
            FieldResolver resolver)
    {
        if (value == null) {
            writer.writeNull();
            return;
        }

        //Indicate the beginning of the struct value, this is how Apache Arrow handles the variable length of Struct types.
        writer.start();
        for (Field nextChild : field.getChildren()) {
            //For each child field that comprises the struct, attempt to extract and write the corresponding value
            //using the FieldResolver.
            Object childValue = resolver.getFieldValue(nextChild, value);
            writeAllValue((FieldWriter) writer, nextChild, allocator, pos, resolver, childValue, true);
        }
        writer.end();
    }

    /**
     * Used to write a Map value.
     *
     * @param allocator The BlockAllocator which can be used to generate Apache Arrow Buffers for types
     * which require conversion to an Arrow Buffer before they can be written using the FieldWriter.
     * @param writer The FieldWriter for the Map field we'd like to write into.
     * @param field The Schema details of the Map Field we are writing into.
     * @param pos The position (row) in the Apache Arrow batch we are writing to.
     * @param value The value we'd like to write as a Map.
     * @param resolver The field resolver that can be used to extract individual Struct Map from the value.
     */
    @VisibleForTesting
    protected static void writeMap(BufferAllocator allocator,
            MapWriter writer,
            Field field,
            int pos,
            Object value,
            FieldResolver resolver)
    {
        //We expect null writes to have been handled earlier so this is a no-op.
        if (value == null) {
            writer.writeNull();
            return;
        }

        List<Field> children = field.getChildren();
        Field keyValueStructField;
        if (children.size() != 1) {
            throw new IllegalStateException("Invalid Arrow Map schema: " + field);
        }
        else {
            keyValueStructField = children.get(0);
            if (!MapVector.DATA_VECTOR_NAME.equals(keyValueStructField.getName()) || !(keyValueStructField.getType() instanceof ArrowType.Struct)) {
                throw new IllegalStateException("Invalid Arrow Map schema: " + field);
            }
        }

        List<Field> keyValueChildren = keyValueStructField.getChildren();
        Field keyField;
        Field valueField;
        if (keyValueChildren.size() != 2) {
            throw new IllegalStateException("Invalid Arrow Map schema: " + field);
        }
        else {
            keyField = keyValueChildren.get(0);
            valueField = keyValueChildren.get(1);
            if (!MapVector.KEY_NAME.equals(keyField.getName()) || !MapVector.VALUE_NAME.equals(valueField.getName())) {
                throw new IllegalStateException("Invalid Arrow Map schema: " + field);
            }
        }

        if (!(value instanceof Map)) {
            value = resolver.getFieldValue(field, value);
        }

        //Indicate the beginning of the Map value, this is how Apache Arrow handles the variable length of map types.
        writer.startMap();
        ((Map<Object, Object>) value).entrySet().forEach(entry -> {
            writer.startEntry();
            Object entryKeyValue = entry.getKey();
            //We dont want to use field resolver to get Field type for map of struct
            //we will delegate the process when we are processing struct instead.
            // TODO: Currently this change is narrow on purpose to target specifically the MAP situation with DyanmoDB,
            //  eventually we will want to unify this behavior across the SDK and the rest of the connectors.
            //  We just want to limit the blast radius from this change for now
            if (Types.getMinorTypeForArrowType(keyField.getType()) != Types.MinorType.STRUCT) {
                entryKeyValue = resolver.getMapKey(keyField, entry.getKey());
            }
            writeAllValue((FieldWriter) writer.key(), keyField, allocator, pos, resolver, entryKeyValue, true);

            Object entryValValue = entry.getValue();
            if (entryValValue != null) {
                if (Types.getMinorTypeForArrowType(valueField.getType()) != Types.MinorType.STRUCT) {
                    entryValValue = resolver.getMapValue(valueField, entryValValue);
                }
                writeAllValue((FieldWriter) writer.value(), valueField, allocator, pos, resolver, entryValValue, true);
            }

            writer.endEntry();
        });
        writer.endMap();
    }

    /**
     *
     * @param writer The FieldWriter for the Map field we'd like to write into.
     * @param field The Schema details of the Map Field we are writing into.
     * @param allocator The BlockAllocator which can be used to generate Apache Arrow Buffers for types
     * @param pos The position (row) in the Apache Arrow batch we are writing to.
     * @param resolver The field resolver that can be used to extract individual Struct Map from the value.
     * @param value The value we'd like to write as a Map.
     * @param fromMapOrStruct Is field from map or struct
     */
    protected static void writeAllValue(FieldWriter writer, Field field, BufferAllocator allocator, int pos, FieldResolver resolver, Object value, boolean fromMapOrStruct)
    {
        switch (Types.getMinorTypeForArrowType(field.getType())) {
            case LIST:
                FieldWriter listFieldWriter = (FieldWriter) (fromMapOrStruct ? writer.list(field.getName()) : writer.list());
                writeList(allocator, listFieldWriter, field, pos, ((List) value), resolver);
                break;
            case STRUCT:
                FieldWriter structFieldWriter = (FieldWriter) (fromMapOrStruct ? writer.struct(field.getName()) : writer.struct());
                writeStruct(allocator, structFieldWriter, field, pos, value, resolver);
                break;
            case MAP:
                FieldWriter mapFieldWriter = (FieldWriter) (fromMapOrStruct ? writer.map(field.getName()) : writer.map());
                writeMap(allocator, mapFieldWriter, field, pos, value, resolver);
                break;
            default:
                writeSimpleValue(writer, field, allocator, value, fromMapOrStruct);
                break;
        }
    }

    /**
     * Used to write an individual value into a field, multiple calls to this method per-cell are expected in order
     * to write the N values of a list of size N.
     *
     * @param writer The FieldWriter (already positioned at the row) that we want to write into.
     * @param field The concrete type of the values.
     * @param allocator The BlockAllocator that can be used for allocating Arrow Buffers for fields which require conversion
     * to Arrow Buff before being written.
     * @param value The value to write.
     * @param fromMapOrStruct write the simple value for non map/struct or map/struct type
     */
    protected static void writeSimpleValue(FieldWriter writer, Field field, BufferAllocator allocator, Object value, boolean fromMapOrStruct)
    {
        ArrowType type = field.getType();
        try {
            switch (Types.getMinorTypeForArrowType(type)) {
                case TIMESTAMPMILLITZ:
                    long dateTimeWithZone;
                    String timezone =  ((ArrowType.Timestamp) type).getTimezone();

                    // Known issue with Lists and Maps of TimeStampMilliTZ. This will throw.
                    TimeStampMilliTZWriter timeStampMilliTZWriter = fromMapOrStruct ? writer.timeStampMilliTZ(field.getName(), timezone) : writer.timeStampMilliTZ();
                    if (value == null) {
                        timeStampMilliTZWriter.writeNull();
                        break;
                    }
                    else if (value instanceof ZonedDateTime) {
                        dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone((ZonedDateTime) value);
                    }
                    else if (value instanceof LocalDateTime) {
                        dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(
                                ((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli(), UTC_ZONE_ID.getId());
                    }
                    else if (value instanceof Date) {
                        long ldtInLong = Instant.ofEpochMilli(((Date) value).getTime())
                                .atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
                        dateTimeWithZone = DateTimeFormatterUtil.packDateTimeWithZone(ldtInLong, UTC_ZONE_ID.getId());
                    }
                    else {
                        dateTimeWithZone = (long) value;
                    }
                    timeStampMilliTZWriter.writeTimeStampMilliTZ(dateTimeWithZone);
                    break;
                case DATEMILLI:
                    DateMilliWriter dateMilliWriter = fromMapOrStruct ? writer.dateMilli(field.getName()) : writer.dateMilli();
                    if (value == null) {
                        dateMilliWriter.writeNull();
                    }
                    else if (value instanceof Date) {
                        dateMilliWriter.writeDateMilli(((Date) value).getTime());
                    }
                    else if (value instanceof LocalDateTime) {
                        dateMilliWriter.writeDateMilli(((LocalDateTime) value).atZone(UTC_ZONE_ID).toInstant().toEpochMilli());
                    }
                    else {
                        dateMilliWriter.writeDateMilli((long) value);
                    }
                    break;
                case DATEDAY:
                    DateDayWriter dateDayWriter = fromMapOrStruct ? writer.dateDay(field.getName()) : writer.dateDay();
                    if (value == null) {
                        dateDayWriter.writeNull();
                    }
                    else if (value instanceof Date) {
                        org.joda.time.Days days = org.joda.time.Days.daysBetween(EPOCH,
                                new org.joda.time.DateTime(((Date) value).getTime()));
                        dateDayWriter.writeDateDay(days.getDays());
                    }
                    else if (value instanceof LocalDate) {
                        int days = (int) ((LocalDate) value).toEpochDay();
                        dateDayWriter.writeDateDay(days);
                    }
                    else if (value instanceof Long) {
                        dateDayWriter.writeDateDay(((Long) value).intValue());
                    }
                    else {
                        dateDayWriter.writeDateDay((int) value);
                    }
                    break;
                case FLOAT8:
                    Float8Writer float8Writer = fromMapOrStruct ? writer.float8(field.getName()) : writer.float8();
                    if (value == null) {
                        float8Writer.writeNull();
                    }
                    else if (value instanceof Integer) {
                        float8Writer.writeFloat8((int) value);
                    }
                    else {
                        float8Writer.writeFloat8((double) value);
                    }
                    break;
                case FLOAT4:
                    Float4Writer float4Writer = fromMapOrStruct ? writer.float4(field.getName()) : writer.float4();
                    if (value == null) {
                        float4Writer.writeNull();
                    }
                    else {
                        float4Writer.writeFloat4((float) value);
                    }
                    break;
                case INT:
                    IntWriter integerWriter = fromMapOrStruct ? writer.integer(field.getName()) : writer.integer();
                    if (value == null) {
                        integerWriter.writeNull();
                    }
                    else if (value != null && value instanceof Long) {
                        //This may seem odd at first but many frameworks (like Presto) use long as the preferred
                        //native java type for representing integers. We do this to keep type conversions simple.
                        integerWriter.writeInt(((Long) value).intValue());
                    }
                    else {
                        integerWriter.writeInt((int) value);
                    }
                    break;
                case TINYINT:
                    TinyIntWriter tinyIntWriter = fromMapOrStruct ? writer.tinyInt(field.getName()) : writer.tinyInt();
                    if (value == null) {
                        tinyIntWriter.writeNull();
                    }
                    else {
                        tinyIntWriter.writeTinyInt((byte) value);
                    }
                    break;
                case SMALLINT:
                    SmallIntWriter smallIntWriter = fromMapOrStruct ? writer.smallInt(field.getName()) : writer.smallInt();
                    if (value == null) {
                        smallIntWriter.writeNull();
                    }
                    else {
                        smallIntWriter.writeSmallInt((short) value);
                    }
                    break;
                case UINT1:
                    UInt1Writer uInt1Writer = fromMapOrStruct ? writer.uInt1(field.getName()) : writer.uInt1();
                    if (value == null) {
                        uInt1Writer.writeNull();
                    }
                    else {
                        uInt1Writer.writeUInt1((byte) value);
                    }
                    break;
                case UINT2:
                    UInt2Writer uInt2Writer = fromMapOrStruct ? writer.uInt2(field.getName()) : writer.uInt2();
                    if (value == null) {
                        uInt2Writer.writeNull();
                    }
                    else {
                        uInt2Writer.writeUInt2((char) value);
                    }
                    break;
                case UINT4:
                    UInt4Writer uInt4Writer = fromMapOrStruct ? writer.uInt4(field.getName()) : writer.uInt4();
                    if (value == null) {
                        uInt4Writer.writeNull();
                    }
                    else {
                        uInt4Writer.writeUInt4((int) value);
                    }
                    break;
                case UINT8:
                    UInt8Writer uInt8Writer = fromMapOrStruct ? writer.uInt8(field.getName()) : writer.uInt8();
                    if (value == null) {
                        uInt8Writer.writeNull();
                    }
                    else {
                        uInt8Writer.writeUInt8((long) value);
                    }
                    break;
                case BIGINT:
                    BigIntWriter bigIntWriter = fromMapOrStruct ? writer.bigInt(field.getName()) : writer.bigInt();
                    if (value == null) {
                        bigIntWriter.writeNull();
                    }
                    else {
                        bigIntWriter.writeBigInt((long) value);
                    }
                    break;
                case VARBINARY:
                    VarBinaryWriter varBinaryWriter = fromMapOrStruct ? writer.varBinary(field.getName()) : writer.varBinary();
                    if (value == null) {
                        varBinaryWriter.writeNull();
                    }
                    else if (value instanceof ArrowBuf) {
                        ArrowBuf buf = (ArrowBuf) value;
                        varBinaryWriter.writeVarBinary(0, (int) (buf.capacity()), buf);
                    }
                    else if (value instanceof byte[]) {
                        byte[] bytes = (byte[]) value;
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            varBinaryWriter.writeVarBinary(0, (int) (buf.readableBytes()), buf);
                        }
                    }
                    break;
                case DECIMAL:
                    int scale = ((ArrowType.Decimal) type).getScale();
                    int precision = ((ArrowType.Decimal) type).getPrecision();
                    DecimalWriter decimalWriter = fromMapOrStruct ? writer.decimal(field.getName(), scale, precision) : writer.decimal();
                    if (value == null) {
                        decimalWriter.writeNull();
                    }
                    else if (value instanceof Double) {
                        BigDecimal bdVal = new BigDecimal((double) value);
                        bdVal = bdVal.setScale(scale, RoundingMode.HALF_UP);
                        decimalWriter.writeDecimal(bdVal);
                    }
                    else {
                        BigDecimal scaledValue = ((BigDecimal) value).setScale(scale, RoundingMode.HALF_UP);
                        decimalWriter.writeDecimal(scaledValue);
                    }
                    break;
                case VARCHAR:
                    VarCharWriter varCharWriter = fromMapOrStruct ? writer.varChar(field.getName()) : writer.varChar();
                    if (value == null) {
                        varCharWriter.writeNull();
                    }
                    else if (value instanceof String || value instanceof Text) {
                        if (value instanceof Text) {
                            value = ((Text) value).toString();
                        }
                        byte[] bytes = ((String) value).getBytes(Charsets.UTF_8);
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            varCharWriter.writeVarChar(0, (int) (buf.readableBytes()), buf);
                        }
                    }
                    else if (value instanceof ArrowBuf) {
                        ArrowBuf buf = (ArrowBuf) value;
                        varCharWriter.writeVarChar(0, (int) (buf.readableBytes()), buf);
                    }
                    else if (value instanceof byte[]) {
                        byte[] bytes = (byte[]) value;
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            varCharWriter.writeVarChar(0, (int) (buf.readableBytes()), buf);
                        }
                    }
                    break;
                case BIT:
                    BitWriter bitWriter = fromMapOrStruct ? writer.bit(field.getName()) : writer.bit();
                    if (value == null) {
                        bitWriter.writeNull();
                    }
                    else if (value instanceof Integer && (int) value > 0) {
                        bitWriter.writeBit(1);
                    }
                    else if (value instanceof Boolean && (boolean) value) {
                        bitWriter.writeBit(1);
                    }
                    else {
                        bitWriter.writeBit(0);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }
        catch (RuntimeException ex) {
            throw new RuntimeException("Unable to write value for field "
                + field.getName() + " using value " + value
                + " with minor type " + Types.getMinorTypeForArrowType(type), ex);
        }
    }

    /**
     * Used to mark a particular cell as null.
     *
     * @param vector The FieldVector to write the null value to.
     * @param pos The position (row) in the FieldVector to mark as null.
     */
    private static void setNullValue(FieldVector vector, int pos)
    {
        switch (vector.getMinorType()) {
            case TIMESTAMPMILLITZ:
                ((TimeStampMilliTZVector) vector).setNull(pos);
                break;
            case DATEMILLI:
                ((DateMilliVector) vector).setNull(pos);
                break;
            case DATEDAY:
                ((DateDayVector) vector).setNull(pos);
                break;
            case FLOAT8:
                ((Float8Vector) vector).setNull(pos);
                break;
            case FLOAT4:
                ((Float4Vector) vector).setNull(pos);
                break;
            case INT:
                ((IntVector) vector).setNull(pos);
                break;
            case TINYINT:
                ((TinyIntVector) vector).setNull(pos);
                break;
            case SMALLINT:
                ((SmallIntVector) vector).setNull(pos);
                break;
            case UINT1:
                ((UInt1Vector) vector).setNull(pos);
                break;
            case UINT2:
                ((UInt2Vector) vector).setNull(pos);
                break;
            case UINT4:
                ((UInt4Vector) vector).setNull(pos);
                break;
            case UINT8:
                ((UInt8Vector) vector).setNull(pos);
                break;
            case BIGINT:
                ((BigIntVector) vector).setNull(pos);
                break;
            case VARBINARY:
                ((VarBinaryVector) vector).setNull(pos);
                break;
            case DECIMAL:
                ((DecimalVector) vector).setNull(pos);
                break;
            case VARCHAR:
                ((VarCharVector) vector).setNull(pos);
                break;
            case BIT:
                ((BitVector) vector).setNull(pos);
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + vector.getMinorType());
        }
    }

    /**
     * In some filtering situations it can be useful to 'unset' a row as an indication to a later processing stage
     * that the row is irrelevant. The mechanism by which we 'unset' a row is actually field type specific and as such
     * this method is not supported for all field types.
     *
     * @param row The row number to unset in the provided Block.
     * @param block The Block where we'd like to unset the specified row.
     */
    public static void unsetRow(int row, Block block)
    {
        for (FieldVector vector : block.getFieldVectors()) {
            switch (vector.getMinorType()) {
                case TIMESTAMPMILLITZ:
                    ((TimeStampMilliTZVector) vector).setNull(row);
                    break;
                case DATEDAY:
                    ((DateDayVector) vector).setNull(row);
                    break;
                case DATEMILLI:
                    ((DateMilliVector) vector).setNull(row);
                    break;
                case TINYINT:
                    ((TinyIntVector) vector).setNull(row);
                    break;
                case UINT1:
                    ((UInt1Vector) vector).setNull(row);
                    break;
                case SMALLINT:
                    ((SmallIntVector) vector).setNull(row);
                    break;
                case UINT2:
                    ((UInt2Vector) vector).setNull(row);
                    break;
                case UINT4:
                    ((UInt4Vector) vector).setNull(row);
                    break;
                case INT:
                    ((IntVector) vector).setNull(row);
                    break;
                case UINT8:
                    ((UInt8Vector) vector).setNull(row);
                    break;
                case BIGINT:
                    ((BigIntVector) vector).setNull(row);
                    break;
                case FLOAT4:
                    ((Float4Vector) vector).setNull(row);
                    break;
                case FLOAT8:
                    ((Float8Vector) vector).setNull(row);
                    break;
                case DECIMAL:
                    ((DecimalVector) vector).setNull(row);
                    break;
                case VARBINARY:
                    ((VarBinaryVector) vector).setNull(row);
                    break;
                case VARCHAR:
                    ((VarCharVector) vector).setNull(row);
                    break;
                case BIT:
                    ((BitVector) vector).setNull(row);
                    break;
                case STRUCT:
                    ((StructVector) vector).setNull(row);
                    break;
                case LIST:
                    UnionListWriter writer = ((ListVector) vector).getWriter();
                    writer.setPosition(row);
                    writer.startList();
                    writer.endList();
                    writer.setValueCount(0);
                    break;
                case MAP:
                    ((MapVector) vector).setNull(row);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + vector.getMinorType());
            }
        }
    }

    @VisibleForTesting
    /**
     * Maps an Arrow Type to a Java class.
     * @param minorType
     * @return Java class mapping the Arrow type
     */
    public static Class getJavaType(Types.MinorType minorType)
    {
        switch (minorType) {
            case TIMESTAMPMILLITZ:
                return ZonedDateTime.class;
            case DATEMILLI:
                return LocalDateTime.class;
            case TINYINT:
            case UINT1:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case UINT2:
                return Character.class;
            case DATEDAY:
                return LocalDate.class;
            case INT:
            case UINT4:
                return Integer.class;
            case UINT8:
            case BIGINT:
                return Long.class;
            case DECIMAL:
                return BigDecimal.class;
            case FLOAT4:
                return Float.class;
            case FLOAT8:
                return Double.class;
            case VARCHAR:
                return String.class;
            case VARBINARY:
                return byte[].class;
            case BIT:
                return Boolean.class;
            case LIST:
                return List.class;
            case STRUCT:
                return Map.class;
            default:
                throw new IllegalArgumentException("Unknown type " + minorType);
        }
    }

    public static final org.joda.time.MutableDateTime EPOCH = new org.joda.time.MutableDateTime();

    static {
        EPOCH.setDate(0);
    }

    private BlockUtils() {}

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private static String bytesToHex(byte[] bytes)
    {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}
