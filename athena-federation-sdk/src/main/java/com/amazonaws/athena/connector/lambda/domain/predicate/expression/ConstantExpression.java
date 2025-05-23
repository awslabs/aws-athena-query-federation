/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.domain.predicate.expression;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.ArrowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class ConstantExpression
        extends FederationExpression
{
    public static final String DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME = "col1";
    
    private final Block valueBlock;

    /**
     * @param valueBlock the value encoded using the native "stack" representation for the given type.
     */
    public ConstantExpression(@JsonProperty("valueBlock") @Nullable Block valueBlock,
                              @JsonProperty("type") ArrowType type)
    {
        super(type);
        this.valueBlock = valueBlock;
    }

    @Nullable
    @JsonProperty("valueBlock")
    public Block getValues()
    {
        return valueBlock;
    }

    @Override
    public List<? extends FederationExpression> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(valueBlock, getType());
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

        ConstantExpression that = (ConstantExpression) o;
        return Objects.equals(this.valueBlock, that.valueBlock)
                && Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return valueBlock + "::" + getType();
    }
}
