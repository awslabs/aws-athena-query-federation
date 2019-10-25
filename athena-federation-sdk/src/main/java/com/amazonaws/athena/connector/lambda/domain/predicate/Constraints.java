package com.amazonaws.athena.connector.lambda.domain.predicate;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Map;

public class Constraints
        implements AutoCloseable
{
    private Map<String, ValueSet> summary;

    @JsonCreator
    public Constraints(@JsonProperty("summary") Map<String, ValueSet> summary)
    {
        this.summary = summary;
    }

    public Map<String, ValueSet> getSummary()
    {
        return summary;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        Constraints that = (Constraints) o;

        return Objects.equal(this.summary, that.summary);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(summary);
    }

    @Override
    public void close()
            throws Exception
    {
        for (ValueSet next : summary.values()) {
            try {
                next.close();
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
