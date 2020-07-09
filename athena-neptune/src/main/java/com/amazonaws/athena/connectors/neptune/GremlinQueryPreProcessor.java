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

public class GremlinQueryPreProcessor {
    public static String STRINGEQUALS = ".has('<key>',eq('<value>'))";

    final static String INTEQUALS = ".has('<key>',eq(<value>))";
    final static String INTGREATERTHAN = ".has('<key>',gt(<value>))";
    final static String INTGREATERTHANEQUALTO = ".has('<key>',gte(<value>))";
    final static String INTLESSTHAN = ".has('<key>',lt(<value>))";
    final static String INTLESSTHANEQUALTO = ".has('<key>',lte(<value>))";

    public static class Operator {
        public static String LESSTHAN = "LESSTHAN";
        public static String GREATERTHAN = "GREATERTHAN";
        public static String EQUALTO = "EQUALTO";
    }

    public static String pickTemplate(String key, String value, String type, Bound bound, String operator) {

        String gremlinQueryPart = "";

        switch (type) {

            case "Int(32, true)":

                if (operator.equals(Operator.GREATERTHAN)) {
                    gremlinQueryPart = bound.equals(Bound.EXACTLY)
                            ? INTGREATERTHANEQUALTO.replace("<key>", key).replace("<value>", value)
                            : INTGREATERTHAN.replace("<key>", key).replace("<value>", value);
                }

                if (operator.equals(Operator.LESSTHAN)) {
                    gremlinQueryPart = bound.equals(Bound.EXACTLY)
                            ? INTLESSTHAN.replace("<key>", key).replace("<value>", value)
                            : INTLESSTHANEQUALTO.replace("<key>", key).replace("<value>", value);
                }

                if (operator.equals(Operator.EQUALTO)) {
                    gremlinQueryPart = INTEQUALS.replace("<key>", key).replace("<value>", value);
                }

                break;

            case "Utf8":

                if (operator.equals(Operator.EQUALTO)) {
                    gremlinQueryPart = STRINGEQUALS.replace("<key>", key).replace("<value>", value);
                }

                break;

            default:

                gremlinQueryPart = "";

        }

        return gremlinQueryPart;
    }
}