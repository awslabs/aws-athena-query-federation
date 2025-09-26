/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.substrait.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a logical expression tree that preserves AND/OR hierarchy from Substrait expressions.
 * This allows proper conversion to MongoDB queries while maintaining the original logical structure.
 */
public class LogicalExpression
{
    private final SubstraitOperator operator;
    private final List<LogicalExpression> children;
    private final ColumnPredicate leafPredicate;

    // Constructor for logical operators (AND, OR, NOT, etc.)
    public LogicalExpression(SubstraitOperator operator, List<LogicalExpression> children)
    {
        this.operator = operator;
        this.children = new ArrayList<>(children);
        this.leafPredicate = null;
    }

    // Constructor for leaf predicates (EQUAL, GREATER_THAN, etc.)
    public LogicalExpression(ColumnPredicate leafPredicate)
    {
        this.operator = leafPredicate.getOperator();
        this.children = null;
        this.leafPredicate = leafPredicate;
    }

    public SubstraitOperator getOperator()
    {
        return operator;
    }

    public List<LogicalExpression> getChildren()
    {
        return children;
    }

    public ColumnPredicate getLeafPredicate()
    {
        return leafPredicate;
    }

    public boolean isLeaf()
    {
        return leafPredicate != null;
    }

    public boolean hasComplexLogic()
    {
        if (isLeaf()) {
            System.out.println(">>> hasComplexLogic: isLeaf=true, returning false");
            return false;
        }
        boolean isComplex = operator == SubstraitOperator.AND || operator == SubstraitOperator.OR;
        System.out.println(">>> hasComplexLogic: operator=" + operator + ", isComplex=" + isComplex);
        return isComplex;
    }
}
