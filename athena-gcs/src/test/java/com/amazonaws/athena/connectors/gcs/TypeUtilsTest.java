/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsFieldResolver.DEFAULT_FIELD_RESOLVER;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class})
public class TypeUtilsTest
{
    @Test
    public void testCoerce()
    {
        assertNull(TypeUtils.coerce(Field.nullable("id", new ArrowType.Int(64, false)), null));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Int(64, false)), 1L));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Int(32, true)), 1.0F));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Int(32, true)), 1.000));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Int(32, true)), 1));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Utf8()), "1L"));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Utf8()), 1));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), 1.00));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), 1));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), 1.0F));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), 1));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), 1.0F));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), 1.000));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Bool()), true));
        assertNotNull(TypeUtils.coerce(Field.notNullable("id", new ArrowType.Date(DateUnit.MILLISECOND)), "2022-01-01"));
    }

    @Test
    public void testFieldResolver()
    {
        Object field = DEFAULT_FIELD_RESOLVER.getFieldValue(Field.notNullable("name", new ArrowType.List()), Map.of("name", List.of(1, 2)));
        Assert.assertNotNull(field);
        Object field1 = DEFAULT_FIELD_RESOLVER.getFieldValue(Field.notNullable("name", new ArrowType.Map(false)), Map.of("name", Map.of("id", 2)));
        Assert.assertNotNull(field1);
    }

    @Test(expected = RuntimeException.class)
    public void testFieldResolverException()
    {
        DEFAULT_FIELD_RESOLVER.getFieldValue(Field.notNullable("name", new ArrowType.Bool()), true);
    }

}
