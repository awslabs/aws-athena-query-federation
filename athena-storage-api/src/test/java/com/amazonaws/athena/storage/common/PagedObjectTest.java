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

import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class PagedObjectTest extends GcsTestBase
{

    @Test
    public void testBuilder()
    {
        PagedObject obj = PagedObject.builder().fileNames(List.of("test")).nextToken("token").build();
        assertEquals(obj.getNextToken(), new PagedObject(List.of("test"), "token").getNextToken());
        assertEquals(obj.getFileNames(), new PagedObject(List.of("test"), "token").getFileNames());
    }

    @Test
    public void testToString()
    {
        assertNotNull(new PagedObject(List.of("test"), "token").toString());
    }
}
