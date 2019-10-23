package com.amazonaws.athena.connector.lambda.domain.predicate;

import java.util.List;

public interface Ranges
{
    int getRangeCount();

    /**
     * @return Allowed non-overlapping predicate ranges sorted in increasing order
     */
    List<Range> getOrderedRanges();

    /**
     * @return Single range encompassing all of allowed the ranges
     */
    Range getSpan();
}
