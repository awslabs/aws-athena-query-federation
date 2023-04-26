/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PaginatedRequestIteratorTest
{
    Function<String, Map<String, String>> fakePageRequest = (pageToken) -> {
        if (pageToken == null) {
            return ImmutableMap.of(
                "data", "aaa",
                "nextPage", "2"
            );
        }
        else if (pageToken.equals("2")) {
            return ImmutableMap.of(
                "data", "bbb",
                "nextPage", "3"
            );
        }
        else if (pageToken.equals("3")) {
            return ImmutableMap.of(
                "data", "ccc"
            );
        }
        return null;
    };

    Function<Map<String, String>, String> getPageToken = (in) -> in.get("nextPage");

    @Test
    public void testIteratesOverAllPages()
    {
        List<String> result = PaginatedRequestIterator.stream(fakePageRequest, getPageToken)
            .map(r -> r.get("data"))
            .collect(Collectors.toList());

        assertEquals(ImmutableList.of("aaa", "bbb", "ccc"), result);
    }

    @Test
    public void testBehavior()
    {
        PaginatedRequestIterator<Map<String, String>> iterator = new PaginatedRequestIterator<Map<String, String>>(fakePageRequest, getPageToken);
        assertTrue(iterator.hasNext());

        // Call hasNext() multiple times to make sure it keeps returning true until we've actually moved ahead
        for (int i = 0; i < 10; i++) {
            assertTrue(iterator.hasNext());
        }

        assertEquals(ImmutableMap.of("data", "aaa", "nextPage", "2"), iterator.next());

        // Call hasNext() multiple times to make sure it keeps returning true until we've actually moved ahead
        for (int i = 0; i < 10; i++) {
            assertTrue(iterator.hasNext());
        }

        assertEquals(ImmutableMap.of("data", "bbb", "nextPage", "3"), iterator.next());
        assertTrue(iterator.hasNext());

        // Call hasNext() multiple times to make sure it keeps returning true until we've actually moved ahead
        for (int i = 0; i < 10; i++) {
            assertTrue(iterator.hasNext());
        }

        assertEquals(ImmutableMap.of("data", "ccc"), iterator.next());
        assertFalse(iterator.hasNext()); // Should not have anything else

        // Should throw
        try {
            iterator.next();
            fail("Expected NoSuchElementException but did not get one");
        } catch (NoSuchElementException ex) {
            // Expected
        }

        // Validate hasNext() is false again
        assertFalse(iterator.hasNext());
    }
}
