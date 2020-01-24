/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.*;

public class TypeUtilsTest
{

    @Test
    public void unsupportedCoerce()
    {
        Object result = TypeUtils.coerce(FieldBuilder.newBuilder("unsupported", Types.MinorType.VARCHAR.getType()).build(), new UnsupportedType());
        assertEquals("UnsupportedType{}", result);
        assertTrue(result instanceof String);
    }

}
