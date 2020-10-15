/*-
 * #%L
 * athena-neptune
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

package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * This class is a Utility class to general gremlin query equivalents of
 * Contraints being passed via AWS Lambda Handler
 */
public final class GremlinQueryPreProcessor 
{
    private GremlinQueryPreProcessor() 
    {
        //Empty private constructor
    }

    public enum Operator {
        LESSTHAN, GREATERTHAN, EQUALTO, NOTEQUALTO
    }

    /**
     * Pick and Process pre-defined templates based on parameters to generate gremlin query
     * @param traversal Gremlin Traversal
     * @param key       Query Condition Key
     * @param value     Query Condition Value
     * @param bound     Query Condition Value Range
     * @param operator  Query Operator representing conditional operators e.g < , <=, >, >= , =
     * @return A Gremlin Query Part equivalent to Contraint.
     */
    public static GraphTraversal<Vertex, Vertex> generateGremlinQueryPart(GraphTraversal<Vertex, Vertex> traversal, String key, String value, ArrowType type, Bound bound, Operator operator) 
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);

        switch (minorType) {
            case BIT:

                traversal = updateTraversal(traversal, bound, operator, key, Boolean.parseBoolean(value));

                break;

            case INT:

                traversal = updateTraversal(traversal, bound, operator, key, Integer.parseInt(value));

                break;

            case BIGINT:

                traversal = updateTraversal(traversal, bound, operator, key, Long.parseLong(value));

                break;

            case FLOAT4:

                traversal = updateTraversal(traversal, bound, operator, key, Float.parseFloat(value));

                break;

            case FLOAT8:

                traversal = updateTraversal(traversal, bound, operator, key, Double.parseDouble(value));

                break;

            case VARCHAR:

                traversal = updateTraversal(traversal, bound, operator, key, value);

                break;
        }
        return traversal;
    }

    /**
     * Pick and Process pre-defined templates based on parameters to generate gremlin query
     * @param traversal Gremlin Traversal
     * @param key       Query Condition Key
     * @param value     Query Condition Value
     * @param bound     Query Condition Value Range
     * @param operator  Query Operator representing conditional operators e.g < , <=, >, >= , =
     * @return A Gremlin Query Part equivalent for all Types
     */
    private static GraphTraversal<Vertex, Vertex> updateTraversal(GraphTraversal<Vertex, Vertex> traversal, Bound bound, Operator operator, String key, Object value) 
    {
        if (operator.equals(Operator.GREATERTHAN)) {
            traversal = bound.equals(Bound.EXACTLY) ? traversal.has(key, P.gte(value))
                    : traversal.has(key, P.gt(value));
        }

        if (operator.equals(Operator.LESSTHAN)) {
            traversal = bound.equals(Bound.EXACTLY) ? traversal.has(key, P.lte(value))
                    : traversal.has(key, P.lt(value));
        }

        if (operator.equals(Operator.EQUALTO)) {
            traversal = traversal.has(key, P.eq(value));
        }

        if (operator.equals(Operator.NOTEQUALTO)) {
            traversal = traversal.has(key, P.neq(value));
        }

        return traversal;
    }
}
