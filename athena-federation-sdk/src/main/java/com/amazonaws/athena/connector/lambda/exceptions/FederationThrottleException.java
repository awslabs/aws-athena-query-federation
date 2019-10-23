package com.amazonaws.athena.connector.lambda.exceptions;

/**
 * Throw this exception if your source is unable to keep up with the rate or concurrency
 * of requests. Athena constantly monitors for performance of federated sources and
 * employs a congestion control mechanism to reduce pressure on sources that may be
 * overwhelmed or unable to keep up. Throwing this exception gives Athena important
 * back pressure information. Alternatively you can reduce the concurrency of the
 * affected Lambda function in the Lambda console which will cause Lambda to generate
 * Throttle exceptions for Athena.
 *
 * @note If you throw this exception after writing any data Athena may fail the operation
 *       to ensure consistency of your results. This is because Athena eagerly processes
 *       your results but is unsure if two identical calls to your source will produce
 *       the exact same result set (including ordering).
 */
public class FederationThrottleException
        extends RuntimeException
{
    public FederationThrottleException()
    {
        super();
    }

    public FederationThrottleException(String message)
    {
        super(message);
    }

    public FederationThrottleException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
