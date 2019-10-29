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
 * Most aspects of this implementation favor using less memory, often at the expense of extra CPU (more looping, etc...).
 * This is because many of the operations like equality are used rarely and only by
 * code paths that have a small number of rows.
 */
public class Block
        extends SchemaAware
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(Block.class);

    private final String allocatorId;   //not included in equality
    private final Schema schema;
    private final VectorSchemaRoot vectorSchema;

    protected Block(String allocatorId, Schema schema, VectorSchemaRoot vectorSchema)
    {
        requireNonNull(allocatorId, "allocatorId is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(vectorSchema, "vectorSchema is null");
        this.allocatorId = allocatorId;
        this.schema = schema;
        this.vectorSchema = vectorSchema;
    }

    public String getAllocatorId()
    {
        return allocatorId;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public void setValue(String fieldName, int row, Object value)
    {
        BlockUtils.setValue(getFieldVector(fieldName), row, value);
    }

    /**
     * Attempts to set the provided value for the given field name and row. If the Block's schema does not
     * contain such a field, this method does nothing and returns false.
     *
     * @param fieldName
     * @param row
     * @param value
     * @return
     */
    public boolean offerValue(String fieldName, int row, Object value)
    {
        FieldVector vector = getFieldVector(fieldName);
        if (vector != null) {
            BlockUtils.setValue(vector, row, value);
            return true;
        }
        return false;
    }

    /**
     * Attempts to set the provided value for the given field name and row. If the Block's schema does not
     * contain such a field, this method does nothing and returns false.
     *
     * @param fieldName
     * @param row
     * @param value
     * @return
     */
    public boolean offerComplexValue(String fieldName, int row, FieldResolver fieldResolver, Object value)
    {
        FieldVector vector = getFieldVector(fieldName);
        if (vector != null) {
            BlockUtils.setComplexValue(vector, row, fieldResolver, value);
            return true;
        }
        return false;
    }

    protected VectorSchemaRoot getVectorSchema()
    {
        return vectorSchema;
    }

    public void setRowCount(int rowCount)
    {
        vectorSchema.setRowCount(rowCount);
    }

    public int getRowCount()
    {
        return vectorSchema.getRowCount();
    }

    //TODO: How will this work for nested fields? I think it works naturally
    public FieldReader getFieldReader(String fieldName)
    {
        return vectorSchema.getVector(fieldName).getReader();
    }

    public FieldVector getFieldVector(String fieldName)
    {
        return vectorSchema.getVector(fieldName);
    }

    public List<FieldReader> getFieldReaders()
    {
        List<FieldReader> readers = new ArrayList<>();
        for (FieldVector next : vectorSchema.getFieldVectors()) {
            readers.add(next.getReader());
        }
        return readers;
    }

    @Transient
    public long getSize()
    {
        long size = 0;
        for (FieldVector next : vectorSchema.getFieldVectors()) {
            size += next.getBufferSize();
        }
        return size;
    }

    public List<FieldVector> getFieldVectors()
    {
        return vectorSchema.getFieldVectors();
    }

    public ArrowRecordBatch getRecordBatch()
    {
        VectorUnloader vectorUnloader = new VectorUnloader(vectorSchema);
        return vectorUnloader.getRecordBatch();
    }

    public void loadRecordBatch(ArrowRecordBatch batch)
    {
        VectorLoader vectorLoader = new VectorLoader(vectorSchema);
        vectorLoader.load(batch);
        batch.close();
    }

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

        //TODO: Test with nested types
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

        //TODO: Test with nested types
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

    @Override
    public int hashCode()
    {
        int hashcode = 0;
        for (Map.Entry<String, String> next : this.schema.getCustomMetadata().entrySet()) {
            hashcode = hashcode + Objects.hashCode(next);
        }

        //TODO: Test with nested types
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
            for (int i = 0; i < rowsToPrint; i++) {
                thisReader.setPosition(i);
                values.add(fieldToString(thisReader));
            }
            helper.add(next.getName(), values);
        }

        return helper.toString();
    }
}
