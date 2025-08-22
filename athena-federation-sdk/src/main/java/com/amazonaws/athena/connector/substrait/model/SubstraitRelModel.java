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
package com.amazonaws.athena.connector.substrait.model;

import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.SortRel;

/**
 * Model class that encapsulates the different types of Substrait relations extracted from a query plan.
 * This class provides a convenient way to access various relational operations (read, filter, project, sort, fetch)
 * that may be present in a Substrait relation tree.
 * 
 * <p>Each relation type corresponds to a SQL operation:
 * <ul>
 *   <li>ReadRel - Table scan (FROM clause)</li>
 *   <li>FilterRel - Row filtering (WHERE clause)</li>
 *   <li>ProjectRel - Column selection (SELECT clause)</li>
 *   <li>SortRel - Ordering (ORDER BY clause)</li>
 *   <li>FetchRel - Row limiting (LIMIT clause)</li>
 * </ul>
 */
public final class SubstraitRelModel
{
    private final ReadRel readRel;
    private final FilterRel filterRel;
    private final ProjectRel projectRel;
    private final SortRel sortRel;
    private final FetchRel fetchRel;

    public SubstraitRelModel(final ReadRel readRel,
                             final FilterRel filterRel,
                             final ProjectRel projectRel,
                             final SortRel sortRel,
                             final FetchRel fetchRel)
    {
        this.readRel = readRel;
        this.filterRel = filterRel;
        this.projectRel = projectRel;
        this.sortRel = sortRel;
        this.fetchRel = fetchRel;
    }

    public ReadRel getReadRel()
    {
        return readRel;
    }

    public FilterRel getFilterRel()
    {
        return filterRel;
    }

    public ProjectRel getProjectRel()
    {
        return projectRel;
    }

    public SortRel getSortRel()
    {
        return sortRel;
    }

    public FetchRel getFetchRel()
    {
        return fetchRel;
    }

    /**
     * Factory method to build a SubstraitRelModel by extracting all relation types from a Substrait relation tree.
     * This method traverses the relation tree and extracts each type of relation that may be present.
     * 
     * @param rel The root Substrait relation to extract from
     * @return A new SubstraitRelModel containing all extracted relations
     * @throws IllegalArgumentException if rel is null
     */
    public static SubstraitRelModel buildSubstraitRelModel(io.substrait.proto.Rel rel)
    {
        if (rel == null) {
            throw new IllegalArgumentException("Substrait relation cannot be null");
        }
        
        ReadRel readRel = SubstraitRelUtils.getReadRel(rel);
        FilterRel filterRel = SubstraitRelUtils.getFilterRel(rel);
        ProjectRel projectRel = SubstraitRelUtils.getProjectRel(rel);
        SortRel sortRel = SubstraitRelUtils.getSortRel(rel);
        FetchRel fetchRel = SubstraitRelUtils.getFetchRel(rel);
        
        return new SubstraitRelModel(readRel, filterRel, projectRel, sortRel, fetchRel);
    }
}
