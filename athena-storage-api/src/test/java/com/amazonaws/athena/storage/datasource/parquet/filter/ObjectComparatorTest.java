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
package com.amazonaws.athena.storage.datasource.parquet.filter;

import com.amazonaws.athena.storage.gcs.GcsTestBase;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.time.LocalDateTime;
import java.util.Date;

import static org.testng.Assert.assertEquals;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class ObjectComparatorTest extends GcsTestBase
{

    @Test
    public void testObjectComparatorString()
    {
        int i = ObjectComparator.compare(new String("test"), new String());
        assertEquals(i, 4);
    }

    @Test
    public void testObjectComparatorInteger()
    {
        int i = ObjectComparator.compare(1, new Integer(1));
        assertEquals(i, 0);
    }

    @Test
    public void testObjectComparatorLong()
    {
        int i = ObjectComparator.compare(1L, new Long(1));
        assertEquals(i, 0);
    }

    @Test
    public void testObjectComparatorFloat()
    {
        int i = ObjectComparator.compare(1.1F, new Float(1.1));
        assertEquals(i, 0);
    }

    @Test
    public void testObjectComparatorDate()
    {
        int i = ObjectComparator.compare(new Date("12/01/2022"), new Date("12/01/2022"));
        assertEquals(i, 0);
    }

    @Test
    public void testObjectComparatorDouble()
    {
        int i = ObjectComparator.compare(1.11D, new Double("1.11"));
        assertEquals(i, 0);
    }

    @Test(expected = UncheckedStorageDatasourceException.class)
    public void testObjectComparatorException()
    {
        int i = ObjectComparator.compare(LocalDateTime.now(), new Double("1.11"));
        assertEquals(i, 0);
    }
}
