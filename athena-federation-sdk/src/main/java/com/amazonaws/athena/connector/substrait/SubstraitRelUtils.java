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

import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.SortRel;

import java.util.Base64;

/**
 * Utility class for working with Substrait relational algebra expressions.
 * Provides methods to traverse and extract specific relation types from Substrait plans.
 */
public final class SubstraitRelUtils
{
    private SubstraitRelUtils()
    {
        // Utility class - prevent instantiation
    }

    /**
     * Interface for extracting specific relation types from Substrait Rel objects.
     * @param <T> The type of relation to extract
     */
    private interface RelExtractor<T>
    {
        /**
         * Checks if the given Rel contains the target relation type.
         * @param rel The Rel to check
         * @return true if the Rel contains the target relation type
         */
        boolean hasRel(io.substrait.proto.Rel rel);

        /**
         * Extracts the target relation from the given Rel.
         * @param rel The Rel to extract from
         * @return The extracted relation
         */
        T getRel(io.substrait.proto.Rel rel);
    }

    /**
     * Recursively traverses a Substrait relation tree to find and extract a specific relation type.
     * 
     * @param rel The root relation to start traversal from
     * @param extractor The extractor that defines how to identify and extract the target relation
     * @param <T> The type of relation to extract
     * @return The first matching relation found during traversal, or null if not found
     */
    private static <T> T traverseAndExtractRel(io.substrait.proto.Rel rel, RelExtractor<T> extractor)
    {
        if (rel == null) {
            return null;
        }

        if (extractor.hasRel(rel)) {
            return extractor.getRel(rel);
        }

        // Traverse to the next relation in the tree
        io.substrait.proto.Rel nextRel = null;
        if (rel.hasFilter()) {
            nextRel = rel.getFilter().getInput();
        }
        else if (rel.hasFetch()) {
            nextRel = rel.getFetch().getInput();
        }
        else if (rel.hasSort()) {
            nextRel = rel.getSort().getInput();
        }
        else if (rel.hasAggregate()) {
            nextRel = rel.getAggregate().getInput();
        }
        else if (rel.hasProject()) {
            nextRel = rel.getProject().getInput();
        }

        return nextRel != null ? traverseAndExtractRel(nextRel, extractor) : null;
    }

    /**
     * Extracts a FilterRel from a Substrait relation tree.
     * 
     * @param rel The root relation to search from
     * @return The first FilterRel found in the relation tree, or null if none exists
     */
    public static FilterRel getFilterRel(io.substrait.proto.Rel rel)
    {
        return traverseAndExtractRel(rel, new RelExtractor<FilterRel>()
        {
            @Override
            public boolean hasRel(io.substrait.proto.Rel r)
            {
                return r.hasFilter();
            }

            @Override
            public FilterRel getRel(io.substrait.proto.Rel r)
            {
                return r.getFilter();
            }
        });
    }

    /**
     * Extracts a FetchRel (LIMIT operation) from a Substrait relation tree.
     * 
     * @param rel The root relation to search from
     * @return The first FetchRel found in the relation tree, or null if none exists
     */
    public static FetchRel getFetchRel(io.substrait.proto.Rel rel)
    {
        return traverseAndExtractRel(rel, new RelExtractor<FetchRel>()
        {
            @Override
            public boolean hasRel(io.substrait.proto.Rel r)
            {
                return r.hasFetch();
            }

            @Override
            public FetchRel getRel(io.substrait.proto.Rel r)
            {
                return r.getFetch();
            }
        });
    }

    /**
     * Extracts a SortRel (ORDER BY operation) from a Substrait relation tree.
     * 
     * @param rel The root relation to search from
     * @return The first SortRel found in the relation tree, or null if none exists
     */
    public static SortRel getSortRel(io.substrait.proto.Rel rel)
    {
        return traverseAndExtractRel(rel, new RelExtractor<SortRel>()
        {
            @Override
            public boolean hasRel(io.substrait.proto.Rel r)
            {
                return r.hasSort();
            }

            @Override
            public SortRel getRel(io.substrait.proto.Rel r)
            {
                return r.getSort();
            }
        });
    }

    /**
     * Extracts a ReadRel (table scan operation) from a Substrait relation tree.
     * 
     * @param rel The root relation to search from
     * @return The first ReadRel found in the relation tree, or null if none exists
     */
    public static ReadRel getReadRel(io.substrait.proto.Rel rel)
    {
        return traverseAndExtractRel(rel, new RelExtractor<ReadRel>()
        {
            @Override
            public boolean hasRel(io.substrait.proto.Rel r)
            {
                return r.hasRead();
            }

            @Override
            public ReadRel getRel(io.substrait.proto.Rel r)
            {
                return r.getRead();
            }
        });
    }

    /**
     * Extracts a ProjectRel (SELECT operation) from a Substrait relation tree.
     * 
     * @param rel The root relation to search from
     * @return The first ProjectRel found in the relation tree, or null if none exists
     */
    public static ProjectRel getProjectRel(io.substrait.proto.Rel rel)
    {
        return traverseAndExtractRel(rel, new RelExtractor<ProjectRel>()
        {
            @Override
            public boolean hasRel(io.substrait.proto.Rel r)
            {
                return r.hasProject();
            }

            @Override
            public ProjectRel getRel(io.substrait.proto.Rel r)
            {
                return r.getProject();
            }
        });
    }

    /**
     * Deserializes a Base64-encoded Substrait plan into a Plan object.
     * 
     * @param planString Base64-encoded string representation of a Substrait plan
     * @return The deserialized Plan object
     * @throws RuntimeException if the plan string cannot be decoded or parsed
     */
    public static Plan deserializeSubstraitPlan(String planString)
    {
        try {
            byte[] planBytes = Base64.getDecoder().decode(planString);
            return Plan.parseFrom(planBytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse Substrait plan", e);
        }
    }
}
