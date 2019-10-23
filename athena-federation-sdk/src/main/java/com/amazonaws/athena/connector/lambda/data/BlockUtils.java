package com.amazonaws.athena.connector.lambda.data;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.codec.Charsets;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

//TODO: Add all types
public class BlockUtils
{
    private static final MutableDateTime EPOCH = new MutableDateTime();

    static {
        EPOCH.setDate(0);
    }

    private BlockUtils() {}

    public static Block newBlock(BlockAllocator allocator, String columnName, ArrowType type, Object... values)
    {
        return newBlock(allocator, columnName, type, Arrays.asList(values));
    }

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

    public static Block newEmptyBlock(BlockAllocator allocator, String columnName, ArrowType type)
    {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addField(columnName, type);
        Schema schema = schemaBuilder.build();
        return allocator.createBlock(schema);
    }

    private static void setNullValue(FieldVector vector, int pos)
    {
        //TODO: add all types
        switch (vector.getMinorType()) {
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

    protected static void writeList(BufferAllocator allocator,
            FieldWriter writer,
            Field field,
            int pos,
            Iterator value,
            FieldResolver resolver)
    {
        Field child = null;
        if (field.getChildren() != null && !field.getChildren().isEmpty()) {
            child = field.getChildren().get(0);
        }

        writer.startList();
        Iterator itr = value;
        while (itr.hasNext()) {
            Object val = itr.next();
            if (val != null) {
                switch (Types.getMinorTypeForArrowType(child.getType())) {
                    case LIST:
                        try {
                            writeList(allocator, (FieldWriter) writer.list(), child, pos, ((List) val).iterator(), resolver);
                        }
                        catch (Exception ex) {
                            throw ex;
                        }
                        break;
                    case STRUCT:
                        writeStruct(allocator, writer.struct(), child, pos, val, resolver);
                        break;
                    default:
                        writeListValue(writer, child.getType(), allocator, val);
                        break;
                }
            }
        }
        writer.endList();
    }

    protected static void writeStruct(BufferAllocator allocator,
            StructWriter writer,
            Field field,
            int pos,
            Object value,
            FieldResolver resolver)
    {
        if (value == null) {
            return;
        }
        writer.start();
        for (Field nextChild : field.getChildren()) {
            Object childValue = resolver.getFieldValue(nextChild, value);
            switch (Types.getMinorTypeForArrowType(nextChild.getType())) {
                case LIST:
                    writeList(allocator,
                            (FieldWriter) writer.list(nextChild.getName()),
                            nextChild,
                            pos,
                            ((List) childValue).iterator(),
                            resolver);
                    break;
                case STRUCT:
                    writeStruct(allocator,
                            writer.struct(nextChild.getName()),
                            nextChild,
                            pos,
                            childValue,
                            resolver);
                    break;
                default:
                    writeStructValue(writer, nextChild, allocator, childValue);
                    break;
            }
        }
        writer.end();
    }

    /**
     * Used to set complex values (Struct, List, etc...) on the provided FieldVector.
     * @param vector The FieldVector into which we should write the provided value.
     * @param pos The row number that the value should be written to.
     * @param resolver The FieldResolver that can be used to map your value to the complex type (mostly for Structs, Maps).
     * @param value The value to write.
     * @note This method incurs more Object overhead (heap churn) than using Arrow's native interface. Users of this Utility
     * should weigh their performance needs vs. the readability / ease of use.
     */
    public static void setComplexValue(FieldVector vector, int pos, FieldResolver resolver, Object value)
    {
        if (vector instanceof ListVector) {
            if (value != null) {
                UnionListWriter writer = ((ListVector) vector).getWriter();
                writer.setPosition(pos);
                writeList(vector.getAllocator(),
                        writer,
                        vector.getField(),
                        pos,
                        ((List) value).iterator(),
                        resolver);
                ((ListVector) vector).setNotNull(pos);
            }
        }
        else if (vector instanceof StructVector) {
            StructWriter writer = ((StructVector) vector).getWriter();
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

            //TODO: add all types
            switch (vector.getMinorType()) {
                case DATEMILLI:
                    if (value instanceof Date) {
                        ((DateMilliVector) vector).setSafe(pos, ((Date) value).getTime());
                    }
                    else {
                        ((DateMilliVector) vector).setSafe(pos, (long) value);
                    }
                    break;
                case DATEDAY:
                    if (value instanceof Date) {
                        Days days = Days.daysBetween(EPOCH, new DateTime(((Date) value).getTime()));
                        ((DateDayVector) vector).setSafe(pos, days.getDays());
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
                    ((TinyIntVector) vector).setSafe(pos, (int) value);
                    break;
                case SMALLINT:
                    ((SmallIntVector) vector).setSafe(pos, (int) value);
                    break;
                case UINT1:
                    ((UInt1Vector) vector).setSafe(pos, (int) value);
                    break;
                case UINT2:
                    ((UInt2Vector) vector).setSafe(pos, (int) value);
                    break;
                case UINT4:
                    ((UInt4Vector) vector).setSafe(pos, (int) value);
                    break;
                case UINT8:
                    ((UInt8Vector) vector).setSafe(pos, (int) value);
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
                    if (value instanceof String) {
                        ((VarCharVector) vector).setSafe(pos, ((String) value).getBytes(Charsets.UTF_8));
                    }
                    else {
                        ((VarCharVector) vector).setSafe(pos, (Text) value);
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
            throw new RuntimeException("Unable to set value for field " + fieldName + " using value " + value, ex);
        }
    }

    //It is extremely annoying that ListWriter and StructWriter don't share a useful ancestor despite having identical methods...hence the DRY violation of this method
    protected static void writeListValue(FieldWriter writer, ArrowType type, BufferAllocator allocator, Object value)
    {
        try {
            //TODO: add all types
            switch (Types.getMinorTypeForArrowType(type)) {
                case DATEMILLI:
                    if (value instanceof Date) {
                        writer.writeDateMilli(((Date) value).getTime());
                    }
                    else {
                        writer.writeDateMilli((long) value);
                    }
                    break;
                case DATEDAY:
                    if (value instanceof Date) {
                        Days days = Days.daysBetween(EPOCH, new DateTime(((Date) value).getTime()));
                        writer.writeDateDay(days.getDays());
                    }
                    else if (value instanceof Long) {
                        writer.writeDateDay(((Long) value).intValue());
                    }
                    else {
                        writer.writeDateDay((int) value);
                    }
                    break;
                case FLOAT8:
                    writer.float8().writeFloat8((double) value);
                    break;
                case FLOAT4:
                    writer.float4().writeFloat4((float) value);
                    break;
                case INT:
                    if (value != null && value instanceof Long) {
                        //This may seem odd at first but many frameworks (like Presto) use long as the preferred
                        //native java type for representing integers. We do this to keep type conversions simple.
                        writer.integer().writeInt(((Long) value).intValue());
                    }
                    else {
                        writer.integer().writeInt((int) value);
                    }
                    break;
                case TINYINT:
                    writer.tinyInt().writeTinyInt((byte) value);
                    break;
                case SMALLINT:
                    writer.smallInt().writeSmallInt((short) value);
                    break;
                case UINT1:
                    writer.uInt1().writeUInt1((byte) value);
                    break;
                case UINT2:
                    writer.uInt2().writeUInt2((char) value);
                    break;
                case UINT4:
                    writer.uInt4().writeUInt4((int) value);
                    break;
                case UINT8:
                    writer.uInt8().writeUInt8((long) value);
                    break;
                case BIGINT:
                    writer.bigInt().writeBigInt((long) value);
                    break;
                case VARBINARY:
                    if (value instanceof ArrowBuf) {
                        ArrowBuf buf = (ArrowBuf) value;
                        writer.varBinary().writeVarBinary(0, buf.capacity(), buf);
                    }
                    else if (value instanceof byte[]) {
                        byte[] bytes = (byte[]) value;
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            writer.varBinary().writeVarBinary(0, buf.readableBytes(), buf);
                        }
                    }
                    break;
                case DECIMAL:
                    int scale = ((ArrowType.Decimal) type).getScale();
                    if (value instanceof Double) {
                        int precision = ((ArrowType.Decimal) type).getPrecision();
                        BigDecimal bdVal = new BigDecimal((double) value);
                        bdVal = bdVal.setScale(scale, RoundingMode.HALF_UP);
                        writer.decimal().writeDecimal(bdVal);
                    }
                    else {
                        BigDecimal scaledValue = ((BigDecimal) value).setScale(scale, RoundingMode.HALF_UP);
                        writer.decimal().writeDecimal(scaledValue);
                    }
                    break;
                case VARCHAR:
                    if (value instanceof String) {
                        byte[] bytes = ((String) value).getBytes(Charsets.UTF_8);
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            writer.varChar().writeVarChar(0, buf.readableBytes(), buf);
                        }
                    }
                    else if (value instanceof ArrowBuf) {
                        ArrowBuf buf = (ArrowBuf) value;
                        writer.varChar().writeVarChar(0, buf.readableBytes(), buf);
                    }
                    else if (value instanceof byte[]) {
                        byte[] bytes = (byte[]) value;
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            writer.varChar().writeVarChar(0, buf.readableBytes(), buf);
                        }
                    }
                    break;
                case BIT:
                    if (value instanceof Integer && (int) value > 0) {
                        writer.bit().writeBit(1);
                    }
                    else if (value instanceof Boolean && (boolean) value) {
                        writer.bit().writeBit(1);
                    }
                    else {
                        writer.bit().writeBit(0);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }
        catch (RuntimeException ex) {
            String fieldName = (writer.getField() != null) ? writer.getField().getName() : "null_vector";
            throw new RuntimeException("Unable to write value for field " + fieldName + " using value " + value, ex);
        }
    }

    //It is extremely annoying that ListWriter and StructWriter don't share a useful ancestor despite having identical methods...hence the DRY violation of this method
    protected static void writeStructValue(StructWriter writer, Field field, BufferAllocator allocator, Object value)
    {
        if (value == null) {
            return;
        }

        ArrowType type = field.getType();
        try {
            //TODO: add all types
            switch (Types.getMinorTypeForArrowType(type)) {
                case DATEMILLI:
                    if (value instanceof Date) {
                        writer.dateMilli(field.getName()).writeDateMilli(((Date) value).getTime());
                    }
                    else {
                        writer.dateMilli(field.getName()).writeDateMilli((long) value);
                    }
                    break;

                case DATEDAY:
                    if (value instanceof Date) {
                        Days days = Days.daysBetween(EPOCH, new DateTime(((Date) value).getTime()));
                        writer.dateDay(field.getName()).writeDateDay(days.getDays());
                    }
                    else if (value instanceof Long) {
                        writer.dateDay(field.getName()).writeDateDay(((Long) value).intValue());
                    }
                    else {
                        writer.dateDay(field.getName()).writeDateDay((int) value);
                    }
                    break;
                case FLOAT8:
                    writer.float8(field.getName()).writeFloat8((double) value);
                    break;
                case FLOAT4:
                    writer.float4(field.getName()).writeFloat4((float) value);
                    break;
                case INT:
                    if (value != null && value instanceof Long) {
                        //This may seem odd at first but many frameworks (like Presto) use long as the preferred
                        //native java type for representing integers. We do this to keep type conversions simple.
                        writer.integer(field.getName()).writeInt(((Long) value).intValue());
                    }
                    else {
                        writer.integer(field.getName()).writeInt((int) value);
                    }
                    break;
                case TINYINT:
                    writer.tinyInt(field.getName()).writeTinyInt((byte) value);
                    break;
                case SMALLINT:
                    writer.smallInt(field.getName()).writeSmallInt((short) value);
                    break;
                case UINT1:
                    writer.uInt1(field.getName()).writeUInt1((byte) value);
                    break;
                case UINT2:
                    writer.uInt2(field.getName()).writeUInt2((char) value);
                    break;
                case UINT4:
                    writer.uInt4(field.getName()).writeUInt4((int) value);
                    break;
                case UINT8:
                    writer.uInt8(field.getName()).writeUInt8((long) value);
                    break;
                case BIGINT:
                    writer.bigInt(field.getName()).writeBigInt((long) value);
                    break;
                case VARBINARY:
                    if (value instanceof ArrowBuf) {
                        ArrowBuf buf = (ArrowBuf) value;
                        writer.varBinary(field.getName()).writeVarBinary(0, buf.capacity(), buf);
                    }
                    else if (value instanceof byte[]) {
                        byte[] bytes = (byte[]) value;
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            writer.varBinary(field.getName()).writeVarBinary(0, buf.readableBytes(), buf);
                        }
                    }
                    break;
                case DECIMAL:
                    int scale = ((ArrowType.Decimal) type).getScale();
                    if (value instanceof Double) {
                        int precision = ((ArrowType.Decimal) type).getPrecision();
                        BigDecimal bdVal = new BigDecimal((double) value);
                        bdVal = bdVal.setScale(scale, RoundingMode.HALF_UP);
                        writer.decimal(field.getName(), scale, precision).writeDecimal(bdVal);
                    }
                    else {
                        BigDecimal scaledValue = ((BigDecimal) value).setScale(scale, RoundingMode.HALF_UP);
                        writer.decimal(field.getName()).writeDecimal(scaledValue);
                    }
                    break;
                case VARCHAR:
                    if (value instanceof String) {
                        byte[] bytes = ((String) value).getBytes(Charsets.UTF_8);
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            writer.varChar(field.getName()).writeVarChar(0, buf.readableBytes(), buf);
                        }
                    }
                    else if (value instanceof ArrowBuf) {
                        ArrowBuf buf = (ArrowBuf) value;
                        writer.varChar(field.getName()).writeVarChar(0, buf.readableBytes(), buf);
                    }
                    else if (value instanceof byte[]) {
                        byte[] bytes = (byte[]) value;
                        try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.writeBytes(bytes);
                            writer.varChar(field.getName()).writeVarChar(0, buf.readableBytes(), buf);
                        }
                    }
                    break;
                case BIT:
                    if (value instanceof Integer && (int) value > 0) {
                        writer.bit(field.getName()).writeBit(1);
                    }
                    else if (value instanceof Boolean && (boolean) value) {
                        writer.bit(field.getName()).writeBit(1);
                    }
                    else {
                        writer.bit(field.getName()).writeBit(0);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }
        catch (RuntimeException ex) {
            throw new RuntimeException("Unable to write value for field " + field.getName() + " using value " + value, ex);
        }
    }

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

    public static String fieldToString(FieldReader reader)
    {

        switch (reader.getMinorType()) {
            case DATEDAY:
                return String.valueOf(reader.readInteger());
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
            default:
                Object obj = reader.readObject();
                return reader.getMinorType() + " - " + ((obj != null) ? obj.getClass().toString() : "null") +
                        "[ " + String.valueOf(obj) + " ]";
        }
    }

    public static void unsetRow(int row, Block block)
    {
        for (FieldVector vector : block.getFieldVectors()) {
            switch (vector.getMinorType()) {
                case FLOAT8:
                    ((Float8Vector) vector).setNull(row);
                    break;
                case INT:
                    ((IntVector) vector).setNull(row);
                    break;
                case UINT2:
                    ((UInt2Vector) vector).setNull(row);
                    break;
                case BIGINT:
                    ((BigIntVector) vector).setNull(row);
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
                default:
                    throw new IllegalArgumentException("Unknown type " + vector.getMinorType());
            }
        }
    }

    /**
     * @param srcBlock
     * @param dstBlock
     * @param firstRow Inclusive
     * @param lastRow Inclusive
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
