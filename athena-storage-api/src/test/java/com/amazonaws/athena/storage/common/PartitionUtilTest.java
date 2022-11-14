/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.common;

import com.amazonaws.athena.storage.GcsTestBase;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Optional;

import static org.testng.Assert.*;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class PartitionUtilTest extends GcsTestBase
{

    @Test
    public void testIsPartionFolder()
    {
        boolean flag = PartitionUtil.isPartitionFolder("month=april");
        assertTrue(flag);
        boolean flag1 = PartitionUtil.isPartitionFolder("month='april'");
        assertTrue(flag1);
        boolean flag2 = PartitionUtil.isPartitionFolder("month=\"april\"");
        assertTrue(flag2);
    }

    @Test
    public void testToString()
    {
        Optional<FieldValue> fieldValue =  PartitionUtil.getPartitionFieldValue("month=april");
        assertNotNull(fieldValue.get());
    }

    @Test
    public  void testRootNameOfPrefix()
    {
        String rootName = PartitionUtil.getRootName("zipcode/StateName='UP'/");
        assertEquals(rootName, "zipcode", "Prefix root name not matched");
    }
}
