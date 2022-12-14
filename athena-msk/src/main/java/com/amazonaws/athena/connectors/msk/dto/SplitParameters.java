/*-
 * #%L
 * Athena MSK Connector
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.msk.dto;

public class SplitParameters
{
    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String START_OFFSET = "startOffset";
    public static final String END_OFFSET = "endOffset";

    public final String topic;
    public final int partition;
    public final long startOffset;
    public final long endOffset;

    // For debug purpose
    public long pulled = 0;
    public long spilled = 0;
    public String info = "";

    /**
     * These parameters need to be set while creating the split and will be used when retrieving records in RecordHandler class.
     * @param topic
     * @param partition
     * @param startOffset
     * @param endOffset
     */
    public SplitParameters(String topic, int partition, long startOffset, long endOffset)
    {
        this.topic = topic;
        this.partition = partition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public String toString()
    {
        return String.format("[topic: %s, partition: %s, start-offset: %s, end-offset: %s]",
                topic, partition, startOffset, endOffset
        );
    }

    /**
     * For debug insight.
     * To know what happens in the RecordHandler across in all the splits.
     * We write log message as comma separated value with intention to create a csv.
     * Simply in the Cloudwatch log we will search by SplitParameters then we will
     * get these related log messages in once place and download as csv, great!
     *
     * The header of the csv file is:
     * debug,topic,partition,start_offset,end_offset,pulled,spilled,missing,info
     *
     * Note that the header missing means - we should not spill these
     */
    public String debug()
    {
        return String.format(
                "SplitParameters,%s,%s,%s,%s,%s,%s,%s,%s %n",
                topic, partition, startOffset, endOffset, pulled, spilled, pulled - spilled, info
        );
    }
}
