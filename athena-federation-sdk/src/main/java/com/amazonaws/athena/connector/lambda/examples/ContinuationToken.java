package com.amazonaws.athena.connector.lambda.examples;

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

/**
 * All items in the "com.amazonaws.athena.connector.lambda.examples" that this class belongs to are part of an
 * 'Example' connector. We do not recommend using any of the classes in this package directly. Instead you can/should
 * copy and modify as needed.
 * <p>
 * This class is used to define the continuation token used by doGetSplits as well as logic for serializing and deserializing
 * said token.
 */
public class ContinuationToken
{
    private static final String CONTINUATION_TOKEN_DIVIDER = ":";
    private final int partition;
    private final int part;

    /**
     * Basic constructor.
     *
     * @param partition The last partition we processed.
     * @param part The next part to process.
     */
    public ContinuationToken(int partition, int part)
    {
        this.partition = partition;
        this.part = part;
    }

    /**
     * The partition in this token.
     *
     * @return int containing the last processed partition.
     */
    public int getPartition()
    {
        return partition;
    }

    /**
     * The part in this token.
     *
     * @return int containing the next part to process.
     */
    public int getPart()
    {
        return part;
    }

    /**
     * Decodes the provided String representation of a ContinuationToken into a ContinuationToken
     *
     * @param token An encoded ContinuationToken.
     * @return The ContinuationToken represented by the token string.
     */
    public static ContinuationToken decode(String token)
    {
        if (token != null) {
            //if we have a continuation token, lets decode it. The format of this token is owned by this class
            String[] tokenParts = token.split(CONTINUATION_TOKEN_DIVIDER);

            if (tokenParts.length != 2) {
                throw new RuntimeException("Unable to decode continuation token " + token);
            }

            int partition = Integer.valueOf(tokenParts[0]);
            return new ContinuationToken(partition, Integer.valueOf(tokenParts[1]));
        }

        //No continuation token present
        return new ContinuationToken(0, 0);
    }

    /**
     * Encodes the provided partition and part into a string representation of ContinuationToken
     *
     * @param partition The last partition we processed.
     * @param part The next part to process.
     * @return The String representation of a ContinuationToken;
     */
    public static String encode(int partition, int part)
    {
        return partition + CONTINUATION_TOKEN_DIVIDER + part;
    }
}
