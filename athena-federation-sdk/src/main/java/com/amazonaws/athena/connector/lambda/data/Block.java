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

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.google.common.base.MoreObjects;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.fieldToString;
import static java.util.Objects.requireNonNull;

/**
 * This class is used to provide a convenient interface for working (reading/writing) Apache Arrow Batches. As such
 * this class is mostly a holder for an Apache Arrow Schema and the associated VectorSchema (used for read/write).
 * The class also includes helper functions for easily loading/unloading data in the form of Arrow Batches.
 *
 * @note While using this class as a holder to encapsulate nuances of Apache Arrow can simplify your programming model
 * and make it easier to get started, using setValue(...), setComplexValue(...), and any of the related helpers to
 * write data to the Apache Arrow structures is less performant than using Apache Arrow's native interfaces. If your usecase
 * and source data can be read in a columnar fashion you can achieve significantly (50% - 200%) better performance by
 * avoiding setValue(...) and setComplexValue(...). In our testing conversion to Apache Arrow was not a significant
 * bottleneck and instead represented extra latency which could be hidden through parallelism and pipelining. This is why
 * we opted to offer these convenience methods.
 * <p>
 * Remember to always close your Block(s) when you are done with them. If you are using a BlockAllocator it is still
 * recommended that you close() Blocks explicitly wherever possible vs. depending on BlockAllocator.close() to free
 * resources. Closing Blocks earlier will reduce peak memory demands and reduce the chance that you exhaust your Apache
 * Arrow memory pool.
 */
public class Block
        extends SchemaAware
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(Block.class);

    //Used to identify which BlockAllocator owns the underlying memory resources used in this Block for debugging purposes.
    //Not included in equality or hashcode.
    private final String allocatorId;
    //The schema of the block
    private final Schema schema;
    //The VectorSchemaRoot which can be used to read/write values to/from the underlying Apache Arrow buffers that
    //for the Arrow Batch of rows.
    private final VectorSchemaRoot vectorSchema;
    //Used to constrain writes to the block, be default we use an emptyEvaluator that allows all writes.
    //Note that we will _NOT_ close this ConstraintEvaluator because we may not own it and the emptyEvaluator
    //has no resources that could leak.
    private ConstraintEvaluator constraintEvaluator = ConstraintEvaluator.emptyEvaluator();

    /**
     * Used by a BlockAllocator to construct a block by setting the key values that a Block 'holds'. Most of the meaningful
     * construction actually takes place within the BlockAllocator that calls this constructor.
     *
     * @param allocatorId Identifier of the BlockAllocator that owns the Block's memory resources.
     * @param schema The schema of the data that can be read/written to the provided VectorSchema.
     * @param vectorSchema Used to read/write values from the Apache Arrow memory buffers owned by this object.
     */
    public Block(String allocatorId, Schema schema, VectorSchemaRoot vectorSchema)
    {
        requireNonNull(allocatorId, "allocatorId is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(vectorSchema, "vectorSchema is null");
        this.allocatorId = allocatorId;
        this.schema = schema;
        this.vectorSchema = vectorSchema;
    }

    /**
     * Used to constrain writes to the Block.
     *
     * @param constraintEvaluator The ConstraintEvaluator to use check if we should allow a value to be written to the Block.
     * @note Setting the ConstraintEvaluator to null disables constraints.
     */
    public void constrain(ConstraintEvaluator constraintEvaluator)
    {
        this.constraintEvaluator = (constraintEvaluator != null) ? constraintEvaluator : ConstraintEvaluator.emptyEvaluator();
    }

    /**
     * Returns the ConstraintEvaluator used by the block.
     */
    public ConstraintEvaluator getConstraintEvaluator()
    {
        return constraintEvaluator;
    }

    public String getAllocatorId()
    {
        return allocatorId;
    }

    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Writes the provided value to the specified field on the specified row. This method does _not_ update the
     * row count on the underlying Apache Arrow VectorSchema. You must call setRowCount(...) to ensure the values
     * your have written are considered 'valid rows' and thus available when you attempt to serialize this Block. This
     * method replies on BlockUtils' field conversion/coercion logic to convert the provided value into a type that
     * matches Apache Arrow's supported serialization format. For more details on coercion please see @BlockUtils
     *
     * @param fieldName The name of the field you wish to write to.
     * @param row The row number to write to. Note that Apache Arrow Blocks begin with row 0 just like a typical array.
     * @param value The value you wish to write.
     * @return True if the value was written to the Block, False if the value was not written due to failing a constraint.
     * @note This method will throw an NPE if you call with with a non-existent field. You can use offerValue(...)
     * to ignore non-existent fields. This can be useful when you are writing results and want to avoid checking
     * if a field has been requested. One such example is when a query projects only a subset of columns and your
     * underlying data store is not columnar.
     */
    public boolean setValue(String fieldName, int row, Object value)
    {
        if (constraintEvaluator.apply(fieldName, value)) {
            BlockUtils.setValue(getFieldVector(fieldName), row, value);
            return true;
        }
        return false;
    }

    /**
     * Attempts to write the provided value to the specified field on the specified row. This method does _not_ update the
     * row count on the underlying Apache Arrow VectorSchema. You must call setRowCount(...) to ensure the values
     * your have written are considered 'valid rows' and thus available when you attempt to serialize this Block. This
     * method replies on BlockUtils' field conversion/coercion logic to convert the provided value into a type that
     * matches Apache Arrow's supported serialization format. For more details on coercion please see @BlockUtils
     *
     * @param fieldName The name of the field you wish to write to.
     * @param row The row number to write to. Note that Apache Arrow Blocks begin with row 0 just like a typical array.
     * @param value The value you wish to write.
     * @return True if the value was written to the Block (even if the field is missing from the Block),
     * False if the value was not written due to failing a constraint.
     * @note This method will take no action if the provided fieldName is not a valid field in this Block's Schema.
     * In such cases the method will return true.
     */
    public boolean offerValue(String fieldName, int row, Object value)
    {
        if (constraintEvaluator.apply(fieldName, value)) {
            FieldVector vector = getFieldVector(fieldName);
            if (vector != null) {
                BlockUtils.setValue(vector, row, value);
            }
            return true;
        }
        return false;
    }

    /**
     * Attempts to set the provided value for the given field name and row. If the Block's schema does not
     * contain such a field, this method does nothing and returns false.
     *
     * @param fieldName The name of the field you wish to write to.
     * @param row The row number to write to. Note that Apache Arrow Blocks begin with row 0 just like a typical array.
     * @param value The value you wish to write.
     * @return True if the value was written to the Block, False if the value was not written due to failing a constraint.
     * @note This method will throw an NPE if you call with with a non-existent field. You can use offerComplexValue(...)
     * to ignore non-existent fields. This can be useful when you are writing results and want to avoid checking
     * if a field has been requested. One such example is when a query projects only a subset of columns and your
     * underlying data store is not columnar.
     */
    public boolean setComplexValue(String fieldName, int row, FieldResolver fieldResolver, Object value)
    {
        FieldVector vector = getFieldVector(fieldName);
        BlockUtils.setComplexValue(vector, row, fieldResolver, value);
        return true;
    }

    /**
     * Attempts to set the provided value for the given field name and row. If the Block's schema does not
     * contain such a field, this method does nothing and returns false.
     *
     * @param fieldName The name of the field you wish to write to.
     * @param row The row number to write to. Note that Apache Arrow Blocks begin with row 0 just like a typical array.
     * @param value The value you wish to write.
     * @return True if the value was written to the Block (even if the field is missing from the Block),
     * False if the value was not written due to failing a constraint.
     * @note This method will take no action if the provided fieldName is not a valid field in this Block's Schema.
     * In such cases the method will return true.
     */
    public boolean offerComplexValue(String fieldName, int row, FieldResolver fieldResolver, Object value)
    {
        FieldVector vector = getFieldVector(fieldName);
        if (vector != null) {
            BlockUtils.setComplexValue(vector, row, fieldResolver, value);
        }
        return true;
    }

    /**
     * Provides access to the Apache Arrow Vector Schema when direct access to Apache Arrow is required.
     *
     * @return The Apache Arrow Vector Schema.
     */
    protected VectorSchemaRoot getVectorSchema()
    {
        return vectorSchema;
    }

    /**
     * Sets the valid row count on the underlying Apache Arrow Vector Schema.
     *
     * @param rowCount The row count to set.
     * @Note If you do not set this value then block may not serialize correctly (too few rows) or rows may
     * not be readable.
     */
    public void setRowCount(int rowCount)
    {
        vectorSchema.setRowCount(rowCount);
    }

    /**
     * Returns the current row count as set by calling setRowCount(...)
     *
     * @return The current valud row count for the Apache Arrow Vector Schema.
     */
    public int getRowCount()
    {
        return vectorSchema.getRowCount();
    }

    /**
     * Provides access to the Apache Arrow FieldReader for the given field name.
     *
     * @param fieldName The name of the field to retrieve.
     * @return The FieldReader that can be used to read values from the Block for the specified field.
     * @note This method throws NPE if the requested field name is not a valid field name in the block's Schema.
     * Additionally, for accessing nested field you must request the parent field and then call reader(String fieldName)
     * on the parent FieldReader. You can find some examples of how to use Apache Arrow for complex/nested types in
     * the UnitTest for this class or BlockUtils.java.
     */
    public FieldReader getFieldReader(String fieldName)
    {
        return vectorSchema.getVector(fieldName).getReader();
    }

    /**
     * Provides access to the Apache Arrow FieldVector which can be used to write values for the given field name.
     *
     * @param fieldName The name of the field to retrieve.
     * @return The FieldVector that can be used to read values from the Block for the specified field or NULL if the field
     * is not in this Block's Schema.
     * @note Additionally, for accessing nested field you must request the parent field and then call the apprioriate
     * method (based on type) to get the child field's FieldVector. You can find some examples of how to use Apache Arrow
     * for complex/nested types in the UnitTest for this class or BlockUtils.java.
     */
    public FieldVector getFieldVector(String fieldName)
    {
        return vectorSchema.getVector(fieldName);
    }

    /**
     * Provides access to the list of all top-level FieldReaders in this Block.
     *
     * @return List<FieldReader> containing the top-level FieldReaders for this block.
     */
    public List<FieldReader> getFieldReaders()
    {
        List<FieldReader> readers = new ArrayList<>();
        for (FieldVector next : vectorSchema.getFieldVectors()) {
            readers.add(next.getReader());
        }
        return readers;
    }

    /**
     * Calculates the current used size in 'bytes' for all Apache Arrow Buffers that comprise the row data for
     * this Block.
     *
     * @return The used bytes of row data in this Block.
     * @note This value is likley smaller than the actually memory held by this Block as it only counts the 'used' portion
     * of the pre-allocated Apache Arrow Buffers. It is generally safer to think about this value as the size of the Block
     * if you serialize it and thus is useful for controlling the size of the Block responses sent to Athena.
     */
    @Transient
    public long getSize()
    {
        long size = 0;
        for (FieldVector next : vectorSchema.getFieldVectors()) {
            size += next.getBufferSize();
        }
        return size;
    }

    /**
     * Provides access to the list of all top-level FieldVectors in this Block.
     *
     * @return List<FieldVectors> containing the top-level FieldVectors for this block.
     */
    public List<FieldVector> getFieldVectors()
    {
        return vectorSchema.getFieldVectors();
    }

    /**
     * Used to unload the Apache Arrow data in this Block in preparation for Serialization.
     *
     * @return An ArrowRecordBatch containing all row data in this Block for use in serializing the Block.
     */
    public ArrowRecordBatch getRecordBatch()
    {
        VectorUnloader vectorUnloader = new VectorUnloader(vectorSchema);
        return vectorUnloader.getRecordBatch();
    }

    /**
     * Used to load Apache Arrow data into this Block after it has been deserialized.
     *
     * @param batch An ArrowRecordBatch containing all row data you'd like to load into this Block.
     * @note The batch is closed after being loaded to avoid memory leaks or data corruption since the buffers
     * associated with the batch are now owned by this Block. Closing the batch essentially decrements the referrence
     * count in the Arrow Allocator.
     */
    public void loadRecordBatch(ArrowRecordBatch batch)
    {
        VectorLoader vectorLoader = new VectorLoader(vectorSchema);
        vectorLoader.load(batch);
        batch.close();
    }

    /**
     * Frees all Apache Arrow Buffers and resources associated with this block.
     *
     * @throws Exception
     */
    @Override
    public void close()
            throws Exception
    {
        this.vectorSchema.close();
    }

    @Override
    protected Schema internalGetSchema()
    {
        return schema;
    }

    /**
     * Provides some basic equality checking for a Block. This method has some draw backs in that is isn't a deep equality
     * and will not work for some large complex blocks. At present this method is useful for testing purposes but may be refactored
     * in a future release.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Block that = (Block) o;

        if (this.schema.getFields().size() != that.schema.getFields().size()) {
            return false;
        }

        if (this.vectorSchema.getRowCount() != that.vectorSchema.getRowCount()) {
            return false;
        }

        try {
            for (Field next : this.schema.getFields()) {
                FieldReader thisReader = vectorSchema.getVector(next.getName()).getReader();
                FieldReader thatReader = that.vectorSchema.getVector(next.getName()).getReader();
                for (int i = 0; i < this.vectorSchema.getRowCount(); i++) {
                    thisReader.setPosition(i);
                    thatReader.setPosition(i);
                    if (ArrowTypeComparator.compare(thisReader, thisReader.readObject(), thatReader.readObject()) != 0) {
                        return false;
                    }
                }
            }
        }
        catch (IllegalArgumentException ex) {
            //can happen when comparator doesn't support the type
            throw ex;
        }
        catch (RuntimeException ex) {
            //There are many differences which can cause an exception, easier to handle them this way
            logger.warn("equals: ", ex);
            return false;
        }

        return true;
    }

    /**
     * Provides some basic equality checking for a Block ignoring ordering. This method has some draw backs in that is
     * isn't a deep equality and will not work for some large complex blocks. At present this method is useful for testing
     * purposes but may be refactored in a future release.
     */
    public boolean equalsAsSet(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Block that = (Block) o;

        if (this.schema.getFields().size() != that.schema.getFields().size()) {
            return false;
        }

        if (this.vectorSchema.getRowCount() != that.vectorSchema.getRowCount()) {
            return false;
        }

        try {
            for (Field next : this.schema.getFields()) {
                FieldReader thisReader = vectorSchema.getVector(next.getName()).getReader();
                FieldReader thatReader = that.vectorSchema.getVector(next.getName()).getReader();
                for (int i = 0; i < this.vectorSchema.getRowCount(); i++) {
                    thisReader.setPosition(i);
                    Types.MinorType type = thisReader.getMinorType();
                    Object val = thisReader.readObject();
                    boolean matched = false;
                    for (int j = 0; j < that.vectorSchema.getRowCount(); j++) {
                        thatReader.setPosition(j);
                        if (ArrowTypeComparator.compare(thatReader, val, thatReader.readObject()) == 0) {
                            matched = true;
                        }
                    }
                    if (!matched) {
                        return false;
                    }
                }
            }
        }
        catch (RuntimeException ex) {
            //There are many differences which can cause an exception, easier to handle them this way
            return false;
        }

        return true;
    }

    /**
     * Provides some basic hashcode capabilities for the Block. This method has some draw backs in that it is difficult
     * to maintain as we add new types and becomes error prone when and slow if missused. This challenge is compounded
     * when understanding the right/wrong ways to use this are not easy to convey.
     */
    @Override
    public int hashCode()
    {
        int hashcode = 0;
        for (Map.Entry<String, String> next : this.schema.getCustomMetadata().entrySet()) {
            hashcode = hashcode + Objects.hashCode(next);
        }

        for (Field next : this.schema.getFields()) {
            FieldReader thisReader = vectorSchema.getVector(next.getName()).getReader();
            for (int i = 0; i < this.vectorSchema.getRowCount(); i++) {
                thisReader.setPosition(i);
                hashcode = 31 * hashcode + Objects.hashCode(thisReader.readObject());
            }
        }
        return hashcode;
    }

    @Override
    public String toString()
    {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("rows", getRowCount());

        int rowsToPrint = this.vectorSchema.getRowCount() > 10 ? 10 : this.vectorSchema.getRowCount();
        for (Field next : this.schema.getFields()) {
            FieldReader thisReader = vectorSchema.getVector(next.getName()).getReader();
            List<String> values = new ArrayList<>();
            int originalReaderPosition = thisReader.getPosition();
            for (int i = 0; i < rowsToPrint; i++) {
                thisReader.setPosition(i);
                values.add(fieldToString(thisReader));
            }
            // reset the position for other reads
            thisReader.setPosition(originalReaderPosition);
            helper.add(next.getName(), values);
        }

        return helper.toString();
    }
}
