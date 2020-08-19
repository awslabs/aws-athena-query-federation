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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * This class is a Utility class to general gremlin query equivalents of
 * Contraints being passed via AWS Lambda Handler
 */
public class GremlinQueryPreProcessor {
    final static String STRINGEQUALS = ".has('<key>',eq('<value>'))";
    final static String STRINGNOTEQUALS = ".has('<key>',neq('<value>'))";

    final static String INTEQUALS = ".has('<key>',eq(<value>))";
    final static String INTGREATERTHAN = ".has('<key>',gt(<value>))";
    final static String INTGREATERTHANEQUALTO = ".has('<key>',gte(<value>))";
    final static String INTLESSTHAN = ".has('<key>',lt(<value>))";
    final static String INTLESSTHANEQUALTO = ".has('<key>',lte(<value>))";
    final static String INTNOTEQUALS = ".has('<key>',neq(<value>))";

    public enum Operator {
        LESSTHAN, GREATERTHAN, EQUALTO, NOTEQUALTO
    }

    /**
     * Pick and Process pre-defined templates based on parameters to generate
     * gremlin query
     * 
     * @param key      Query Condition Key
     * @param value    Query Condition Value
     * @param bound    Query Condition Value Range
     * @param operator Query Operator representing conditional operators e.g < ,
     *                 <=, >, >= , =
     * 
     * @return A Gremlin Query Part equivalent to Contraint.
     */
    // public static String generateGremlinQueryPart(String key, String value,
    // String type, Bound bound,
    // Operator operator) {

    public static GraphTraversal<Vertex, Vertex> generateGremlinQueryPart(GraphTraversal<Vertex, Vertex> traversal,
            String key, String value, String type, Bound bound, Operator operator) {

        switch (type) {

            case "Int(32, true)":

                if (operator.equals(Operator.GREATERTHAN)) {

                    traversal = bound.equals(Bound.EXACTLY) ? traversal.has(key, P.gte(Integer.parseInt(value)))
                            : traversal.has(key, P.gt(Integer.parseInt(value)));
                }

                if (operator.equals(Operator.LESSTHAN)) {

                    traversal = bound.equals(Bound.EXACTLY) ? traversal.has(key, P.lte(Integer.parseInt(value)))
                            : traversal.has(key, P.lt(Integer.parseInt(value)));
                }

                if (operator.equals(Operator.EQUALTO)) {

                    traversal = traversal.has(key, P.eq(Integer.parseInt(value)));
                }

                if (operator.equals(Operator.NOTEQUALTO)) {

                    traversal = traversal.has(key, P.neq(Integer.parseInt(value)));
                }

                break;

            case "Utf8":

                if (operator.equals(Operator.EQUALTO)) {

                    traversal = traversal.has(key, P.eq(value));
                }

                if (operator.equals(Operator.NOTEQUALTO)) {

                    traversal = traversal.has(key, P.neq(value));
                }

                break;

        }

        return traversal;
    }
}