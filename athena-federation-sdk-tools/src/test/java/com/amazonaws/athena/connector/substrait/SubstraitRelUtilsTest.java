/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.substrait;

import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SortRel;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class SubstraitRelUtilsTest
{
    @Test
    void testGetFilterRelFound()
    {
        FilterRel expectedFilter = FilterRel.newBuilder().build();
        Rel rel = Rel.newBuilder()
                .setFilter(expectedFilter)
                .build();
        
        FilterRel result = SubstraitRelUtils.getFilterRel(rel);
        
        assertEquals(expectedFilter, result);
    }

    @Test
    void testGetFilterRelNotFound()
    {
        Rel rel = Rel.newBuilder()
                .setRead(ReadRel.newBuilder().build())
                .build();
        
        FilterRel result = SubstraitRelUtils.getFilterRel(rel);
        
        assertNull(result);
    }

    @Test
    void testGetFilterRelWithNestedStructure()
    {
        FilterRel expectedFilter = FilterRel.newBuilder().build();
        Rel innerRel = Rel.newBuilder()
                .setFilter(expectedFilter)
                .build();
        Rel outerRel = Rel.newBuilder()
                .setProject(ProjectRel.newBuilder()
                        .setInput(innerRel)
                        .build())
                .build();
        
        FilterRel result = SubstraitRelUtils.getFilterRel(outerRel);
        
        assertEquals(expectedFilter, result);
    }

    @Test
    void testGetFetchRelFound()
    {
        FetchRel expectedFetch = FetchRel.newBuilder().setCount(10).build();
        Rel rel = Rel.newBuilder()
                .setFetch(expectedFetch)
                .build();
        
        FetchRel result = SubstraitRelUtils.getFetchRel(rel);
        
        assertEquals(expectedFetch, result);
    }

    @Test
    void testGetFetchRelNotFound()
    {
        Rel rel = Rel.newBuilder()
                .setRead(ReadRel.newBuilder().build())
                .build();
        
        FetchRel result = SubstraitRelUtils.getFetchRel(rel);
        
        assertNull(result);
    }

    @Test
    void testGetSortRelFound()
    {
        SortRel expectedSort = SortRel.newBuilder().build();
        Rel rel = Rel.newBuilder()
                .setSort(expectedSort)
                .build();
        
        SortRel result = SubstraitRelUtils.getSortRel(rel);
        
        assertEquals(expectedSort, result);
    }

    @Test
    void testGetSortRelNotFound()
    {
        Rel rel = Rel.newBuilder()
                .setRead(ReadRel.newBuilder().build())
                .build();
        
        SortRel result = SubstraitRelUtils.getSortRel(rel);
        
        assertNull(result);
    }

    @Test
    void testGetReadRelFound()
    {
        ReadRel expectedRead = ReadRel.newBuilder().build();
        Rel rel = Rel.newBuilder()
                .setRead(expectedRead)
                .build();
        
        ReadRel result = SubstraitRelUtils.getReadRel(rel);
        
        assertEquals(expectedRead, result);
    }

    @Test
    void testGetReadRelNotFound()
    {
        Rel rel = Rel.newBuilder()
                .setFilter(FilterRel.newBuilder().build())
                .build();
        
        ReadRel result = SubstraitRelUtils.getReadRel(rel);
        
        assertNull(result);
    }

    @Test
    void testGetProjectRelFound()
    {
        ProjectRel expectedProject = ProjectRel.newBuilder().build();
        Rel rel = Rel.newBuilder()
                .setProject(expectedProject)
                .build();
        
        ProjectRel result = SubstraitRelUtils.getProjectRel(rel);
        
        assertEquals(expectedProject, result);
    }

    @Test
    void testGetProjectRelNotFound()
    {
        Rel rel = Rel.newBuilder()
                .setRead(ReadRel.newBuilder().build())
                .build();
        
        ProjectRel result = SubstraitRelUtils.getProjectRel(rel);
        
        assertNull(result);
    }

    @Test
    void testDeserializeSubstraitPlanValid()
    {
        Plan originalPlan = Plan.newBuilder().build();
        byte[] planBytes = originalPlan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        
        Plan result = SubstraitRelUtils.deserializeSubstraitPlan(encodedPlan);
        
        assertEquals(originalPlan, result);
    }

    @Test
    void testDeserializeSubstraitPlanInvalidBase64()
    {
        String invalidBase64 = "invalid-base64-string!@#";
        
        assertThrows(RuntimeException.class, () -> 
            SubstraitRelUtils.deserializeSubstraitPlan(invalidBase64));
    }

    @Test
    void testDeserializeSubstraitPlanInvalidProtobuf()
    {
        String invalidProtobuf = Base64.getEncoder().encodeToString("invalid protobuf data".getBytes());
        
        assertThrows(RuntimeException.class, () -> 
            SubstraitRelUtils.deserializeSubstraitPlan(invalidProtobuf));
    }

    @Test
    void testTraversalWithNullRel()
    {
        FilterRel result = SubstraitRelUtils.getFilterRel(null);
        
        assertNull(result);
    }

    @Test
    void testComplexNestedTraversal()
    {
        ReadRel expectedRead = ReadRel.newBuilder().build();
        
        // Create nested structure: Fetch -> Sort -> Filter -> Project -> Read
        Rel readRel = Rel.newBuilder().setRead(expectedRead).build();
        Rel projectRel = Rel.newBuilder()
                .setProject(ProjectRel.newBuilder().setInput(readRel).build())
                .build();
        Rel filterRel = Rel.newBuilder()
                .setFilter(FilterRel.newBuilder().setInput(projectRel).build())
                .build();
        Rel sortRel = Rel.newBuilder()
                .setSort(SortRel.newBuilder().setInput(filterRel).build())
                .build();
        Rel fetchRel = Rel.newBuilder()
                .setFetch(FetchRel.newBuilder().setInput(sortRel).build())
                .build();
        
        ReadRel result = SubstraitRelUtils.getReadRel(fetchRel);
        
        assertEquals(expectedRead, result);
    }
}