/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.domain.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static java.util.Objects.requireNonNull;

public class QueryPlan
{
    String substraitVersion;
    String substraitPlan;
    String sqlConformance;

    /**
     *
     * @param substraitVersion
     * @param substraitPlan
     * @param sqlConformance
     */
    @JsonCreator
    public QueryPlan(@JsonProperty("substraitVersion") String substraitVersion,
                     @JsonProperty("substraitPlan") String substraitPlan,
                     @JsonProperty("sqlConformance") String sqlConformance)
    {
        requireNonNull(substraitPlan, "SubstraitPlan is null");
        this.substraitVersion = substraitVersion;
        this.substraitPlan = substraitPlan;
        this.sqlConformance = sqlConformance;
    }

    public String getSubstraitVersion()
    {
        return substraitVersion;
    }

    public String getSubstraitPlan()
    {
        return substraitPlan;
    }

    public String getSqlConformance()
    {
        return sqlConformance;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("substraitVersion", substraitVersion)
                .add("substraitPlan", substraitPlan)
                .add("sqlConformance", sqlConformance)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryPlan that = (QueryPlan) o;
        return Objects.equal(this.substraitVersion, that.substraitVersion) &&
                Objects.equal(this.substraitPlan, that.substraitPlan) &&
                Objects.equal(this.sqlConformance, that.sqlConformance);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.substraitVersion, this.substraitPlan, this.sqlConformance);
    }
}
