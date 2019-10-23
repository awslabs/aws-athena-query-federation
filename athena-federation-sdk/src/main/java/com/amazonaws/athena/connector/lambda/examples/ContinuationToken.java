package com.amazonaws.athena.connector.lambda.examples;

public class ContinuationToken
{
    private static final String CONTINUATION_TOKEN_DIVIDER = ":";
    private final int partition;
    private final int part;

    public ContinuationToken(int partition, int part)
    {
        this.partition = partition;
        this.part = part;
    }

    public int getPartition()
    {
        return partition;
    }

    public int getPart()
    {
        return part;
    }

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

    public static String encode(int partition, int part)
    {
        return partition + CONTINUATION_TOKEN_DIVIDER + part;
    }
}
