package com.amazonaws.athena.connector.lambda.domain.predicate;

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
