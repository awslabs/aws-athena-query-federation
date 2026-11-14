/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnumsTest {

    private static final int GRAPH_TYPE_COUNT = 2;
    private static final int TABLE_SCHEMA_META_TYPE_COUNT = 3;
    private static final String PROPERTYGRAPH_VALUE = "PROPERTYGRAPH";
    private static final String RDF_VALUE = "RDF";
    private static final String VERTEX_VALUE = "VERTEX";
    private static final String EDGE_VALUE = "EDGE";
    private static final String VIEW_VALUE = "VIEW";
    private static final String INVALID_VALUE = "INVALID";

    @Test
    public void graphTypeValues_ValidEnumValues_ReturnsCorrectEnumInstances() {
        assertEquals(GRAPH_TYPE_COUNT, Enums.GraphType.values().length);
        assertEquals(Enums.GraphType.PROPERTYGRAPH, Enums.GraphType.valueOf(PROPERTYGRAPH_VALUE));
        assertEquals(Enums.GraphType.RDF, Enums.GraphType.valueOf(RDF_VALUE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void graphTypeValueOf_InvalidEnumValue_ThrowsIllegalArgumentException() {
        Enums.GraphType.valueOf(INVALID_VALUE);
    }

    @Test
    public void tableSchemaMetaTypeValues_ValidEnumValues_ReturnsCorrectEnumInstances() {
        assertEquals(TABLE_SCHEMA_META_TYPE_COUNT, Enums.TableSchemaMetaType.values().length);
        assertEquals(Enums.TableSchemaMetaType.VERTEX, Enums.TableSchemaMetaType.valueOf(VERTEX_VALUE));
        assertEquals(Enums.TableSchemaMetaType.EDGE, Enums.TableSchemaMetaType.valueOf(EDGE_VALUE));
        assertEquals(Enums.TableSchemaMetaType.VIEW, Enums.TableSchemaMetaType.valueOf(VIEW_VALUE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void tableSchemaMetaTypeValueOf_InvalidEnumValue_ThrowsIllegalArgumentException() {
        Enums.TableSchemaMetaType.valueOf(INVALID_VALUE);
    }
} 