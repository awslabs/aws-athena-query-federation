/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.splits;

import org.apache.commons.lang3.Validate;

/**
 * Integer splits iterator. Guarantees number of splits expected of a split range. Uses `(high-low)/numSplits + 1` as step to calculate splits. For a non-zero remainder `r`, extra
 * value will be added to first `r` splits.
 *
 * Example: [1, 10] as input split range
 * 1. expected splits = 2, remainder = 0
 *  Splits = [1,5], [6,10]
 *
 * 2. expected splits = 3, remainder = 1
 *  Splits = [1,4], [5,7], [8,10] // note that the remainder gets added in first split only.
 *
 * 3. expected splits = 4, remainder = 2
 *  Splits = [1,3], [4,6], [7,8], [9,10] // note that the remainder gets distributed in first two splits.
 */
public class IntegerSplitter
        implements Splitter<Integer>
{
    private final SplitInfo<Integer> splitInfo;
    private int current;
    private int step;
    private int remainder;
    private int currentSplit;

    /**
     * @param splitInfo split information. E.g. split range, expected splits, column name.
     */
    public IntegerSplitter(SplitInfo<Integer> splitInfo)
    {
        this.splitInfo = Validate.notNull(splitInfo);
        this.current = splitInfo.getSplitRange().getLow();
        Validate.isTrue(splitInfo.getSplitRange().getHigh() >= splitInfo.getSplitRange().getLow(), "high is lower than low");
        if (splitInfo.getSplitRange().getHigh().equals(splitInfo.getSplitRange().getLow())) {
            this.step = 0;
            this.remainder = 1;
        }
        else {
            int diff = splitInfo.getSplitRange().getHigh() - splitInfo.getSplitRange().getLow() + 1;
            int numSplits = splitInfo.getNumSplits();

            this.remainder = diff % numSplits;
            this.step = diff / numSplits;
        }
        this.currentSplit = 1;
    }

    @Override
    public boolean hasNext()
    {
        if (this.step <= 1) {
            return this.current <= splitInfo.getSplitRange().getHigh();
        }
        return this.current < splitInfo.getSplitRange().getHigh();
    }

    @Override
    public SplitRange<Integer> next()
    {
        // subtraction due to closed interval and inclusive endpoints.
        int high = this.current + step - 1;
        high += this.remainder >= this.currentSplit ? 1 : 0;
        int low = this.current;
        if (high > this.splitInfo.getSplitRange().getHigh()) {
            high = this.splitInfo.getSplitRange().getHigh();
        }

        this.current = high + 1;
        this.currentSplit++;

        return new SplitRange<>(low, high);
    }

    @Override
    public String nextRangeClause()
    {
        SplitRange<Integer> splitRange = next();
        return String.format("(%s >= %s AND %s <= %s)", this.splitInfo.getColumnName(), splitRange.getLow(), splitInfo.getColumnName(), splitRange.getHigh());
    }
}
