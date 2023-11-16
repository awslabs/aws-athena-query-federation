package com.amazonaws.athena.connector.lambda.domain.predicate;

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

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Container which holds and maps column names to the corresponding constraint (e.g. ValueSet).
 *
 * @note Only associative predicates are supported. Where relevant, Athena will supply you with the associative
 * portion of the query predicate so that you can perform filtering or push the predicate into your source system
 * for even better performance. It is important to note that the predicate is not always the query's full predicate.
 * For example, if the query's predicate was "where (col0 < 1 or col1 < 10) and col2 + 10 < 100 and function(col3) > 19"
 * only the "col0 < 1 or col1 < 10" will be supplied to you at this time. We are still considering the best form for
 * supplying connectors with a more complete view of the query and its predicate. We expect a future release to  provide
 * full predicates to connectors and lets the connector decide which parts of the predicate it is capable of applying
 */
public class Constraints
        implements AutoCloseable
{
    public static final long DEFAULT_NO_LIMIT = -1;

    private Map<String, ValueSet> summary;
    private List<FederationExpression> expression;
    private final List<OrderByField> orderByClause;
    private long limit;

    @Deprecated
    public Constraints(Map<String, ValueSet> summary) 
    {
        this(summary, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT);
    }

    @JsonCreator
    public Constraints(@JsonProperty("summary") Map<String, ValueSet> summary,
                       @JsonProperty("expression") List<FederationExpression> expression,
                       @JsonProperty("orderByClause") List<OrderByField> orderByClause,
                       @JsonProperty("limit") long limit)
    {
        this.summary = summary;
        this.expression = expression;
        this.orderByClause = orderByClause;
        this.limit = limit;
    }

    /**
     * Provides access to the associative predicates that are part of the Constraints.
     *
     * @return A Map of column name to ValueSet representing the associative predicates on each column.
     */
    public Map<String, ValueSet> getSummary()
    {
        return summary;
    }

    public List<FederationExpression> getExpression()
    {
        return expression;
    }

    public long getLimit()
    {
        return limit;
    }

    public boolean hasLimit()
    {
        return this.limit > DEFAULT_NO_LIMIT;
    }

    public List<OrderByField> getOrderByClause()
    {
        return this.orderByClause;
    }

    public boolean hasNonEmptyOrderByClause()
    {
        return this.orderByClause != null && this.orderByClause.size() > 0;
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

        Constraints that = (Constraints) o;

        return Objects.equal(this.summary, that.summary) &&
                Objects.equal(this.expression, that.expression) &&
                Objects.equal(this.orderByClause, that.orderByClause) &&
                Objects.equal(this.limit, that.limit);
    }

    @Override
    public String toString()
    {
        return "Constraints{" +
                "summary=" + summary +
                "expression=" + expression +
                "orderByClause=" + orderByClause +
                "limit=" + limit +
                '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(summary, expression, orderByClause, limit);
    }

    @Override
    public void close()
    {
        for (ValueSet next : summary.values()) {
            try {
                next.close();
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
