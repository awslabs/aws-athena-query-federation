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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class IntegerSplitterTest
{
    private SplitRange<Integer> inputRange;
    private int numSplits;
    private List<SplitRange<Integer>> expectedRanges;
    private List<String> expectedClauses;

    public IntegerSplitterTest(final SplitRange<Integer> inputRange, final int numSplits, final List<SplitRange<Integer>> expectedRanges, final List<String> expectedClauses) {
        this.inputRange = inputRange;
        this.numSplits = numSplits;
        this.expectedRanges = expectedRanges;
        this.expectedClauses = expectedClauses;
    }

    @Test
    public void splitTest()
    {
        SplitInfo<Integer> splitInfo = new SplitInfo<>(this.inputRange, "testColumn", Types.INTEGER, this.numSplits);
        IntegerSplitter integerSplitter = new IntegerSplitter(splitInfo);

        List<SplitRange<Integer>> splitRanges = ImmutableList.copyOf(integerSplitter);
        Assert.assertEquals(this.expectedRanges, splitRanges);

        SplitInfo<Integer> splitClauseInfo = new SplitInfo<>(this.inputRange, "testColumn", Types.INTEGER, this.numSplits);
        IntegerSplitter integerClauseSplitter = new IntegerSplitter(splitClauseInfo);

        List<String> splitClauses = new ArrayList<>();
        while(integerClauseSplitter.hasNext()) {
            splitClauses.add(integerClauseSplitter.nextRangeClause());
        }
        Assert.assertEquals(this.expectedClauses, splitClauses);
    }

    @Parameterized.Parameters(name = "{index}: Test with inputRange={0}, numSplits ={1}, expectedRanges is:{2}")
    public static Collection<Object[]> data()
    {
        Object[][] data = new Object[][]{
                {new SplitRange<>(1,10), 2, Arrays.asList(new SplitRange<>(1,5), new SplitRange<>(6, 10)),
                        Arrays.asList("(testColumn >= 1 AND testColumn <= 5)", "(testColumn >= 6 AND testColumn <= 10)")},
                {new SplitRange<>(1,2), 10, Arrays.asList(new SplitRange<>(1,1), new SplitRange<>(2, 2)),
                        Arrays.asList("(testColumn >= 1 AND testColumn <= 1)", "(testColumn >= 2 AND testColumn <= 2)")},
                {new SplitRange<>(1,2), 2, Arrays.asList(new SplitRange<>(1,1), new SplitRange<>(2, 2)),
                        Arrays.asList("(testColumn >= 1 AND testColumn <= 1)", "(testColumn >= 2 AND testColumn <= 2)")},
                {new SplitRange<>(1,10), 1, Collections.singletonList(new SplitRange<>(1, 10)),
                        Collections.singletonList("(testColumn >= 1 AND testColumn <= 10)")},
                {new SplitRange<>(1,10), 0, Collections.singletonList(new SplitRange<>(1, 10)),
                        Collections.singletonList("(testColumn >= 1 AND testColumn <= 10)")},
                {new SplitRange<>(1,10), -10, Collections.singletonList(new SplitRange<>(1, 10)),
                        Collections.singletonList("(testColumn >= 1 AND testColumn <= 10)")},
                {new SplitRange<>(1,10), 3, Arrays.asList(new SplitRange<>(1,4), new SplitRange<>(5, 7), new SplitRange<>(8, 10)),
                        Arrays.asList("(testColumn >= 1 AND testColumn <= 4)", "(testColumn >= 5 AND testColumn <= 7)", "(testColumn >= 8 AND testColumn <= 10)")},
                {new SplitRange<>(1,1), 2, Collections.singletonList(new SplitRange<>(1,1)), Collections.singletonList("(testColumn >= 1 AND testColumn <= 1)")}
        };
        return Arrays.asList(data);
    }
}
