package com.amazonaws.athena.connector.lambda;

/*-
 * #%L
 * athena-cloudwatch
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

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ThrottlingInvoker
{
    private static final Logger logger = LoggerFactory.getLogger(ThrottlingInvoker.class);

    private static final String THROTTLE_INITIAL_DELAY_MS = "throttle_initial_delay_ms";
    private static final String THROTTLE_MAX_DELAY_MS = "throttle_max_delay_ms";
    private static final String THROTTLE_DECREASE_FACTOR = "throttle_decrease_factor";
    private static final String THROTTLE_INCREASE_MS = "throttle_increase_ms";

    private static final long DEFAULT_INITIAL_DELAY_MS = 10;
    private static final long DEFAULT_MAX_DELAY_MS = 1_000;
    private static final double DEFAULT_DECREASE_FACTOR = 0.5D;
    private static final long DEFAULT_INCREASE_MS = 10;

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double decrease;
    private final long increase;
    private final ExceptionFilter filter;
    private final AtomicReference<BlockSpiller> spillerRef;
    private final AtomicLong delay = new AtomicLong(0);
    private volatile State state = State.FAST_START;

    public enum State
    {FAST_START, CONGESTED, AVOIDANCE}

    public interface ExceptionFilter
    {
        boolean isMatch(Exception ex);
    }

    public ThrottlingInvoker(Builder builder)
    {
        this(builder.initialDelayMs,
                builder.maxDelayMs,
                builder.decrease,
                builder.increase,
                builder.filter,
                builder.spiller);
    }

    public ThrottlingInvoker(long initialDelayMs,
            long maxDelayMs,
            double decrease,
            long increase,
            ExceptionFilter filter,
            BlockSpiller spiller)
    {
        if (decrease > 1 || decrease < .001) {
            throw new IllegalArgumentException("decrease was " + decrease + " but should be between .001 and 1");
        }

        if (maxDelayMs < 1) {
            throw new IllegalArgumentException("maxDelayMs was " + maxDelayMs + " but must be >= 1");
        }

        if (increase < 1) {
            throw new IllegalArgumentException("increase was " + increase + " but must be >= 1");
        }

        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.decrease = decrease;
        this.increase = increase;
        this.filter = filter;
        this.spillerRef = new AtomicReference<>(spiller);
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static Builder newDefaultBuilder(ExceptionFilter filter)
    {
        long initialDelayMs = (System.getenv(THROTTLE_INITIAL_DELAY_MS) != null) ?
                Long.parseLong(System.getenv(THROTTLE_INITIAL_DELAY_MS)) : DEFAULT_INITIAL_DELAY_MS;
        long maxDelayMs = (System.getenv(THROTTLE_MAX_DELAY_MS) != null) ?
                Long.parseLong(System.getenv(THROTTLE_MAX_DELAY_MS)) : DEFAULT_MAX_DELAY_MS;
        double decreaseFactor = (System.getenv(THROTTLE_DECREASE_FACTOR) != null) ?
                Long.parseLong(System.getenv(THROTTLE_DECREASE_FACTOR)) : DEFAULT_DECREASE_FACTOR;
        long increase = (System.getenv(THROTTLE_INCREASE_MS) != null) ?
                Long.parseLong(System.getenv(THROTTLE_INCREASE_MS)) : DEFAULT_INCREASE_MS;

        return newBuilder()
                .withInitialDelayMs(initialDelayMs)
                .withMaxDelayMs(maxDelayMs)
                .withDecrease(decreaseFactor)
                .withIncrease(increase)
                .withFilter(filter);
    }

    public <T> T invoke(Callable<T> callable)
            throws TimeoutException
    {
        return invoke(callable, 0);
    }

    public <T> T invoke(Callable<T> callable, long timeoutMillis)
            throws TimeoutException
    {
        long startTime = System.currentTimeMillis();
        do {
            try {
                applySleep();
                T result = callable.call();
                handleAvoidance();
                return result;
            }
            catch (Exception ex) {
                if (!filter.isMatch(ex)) {
                    //The exception did not match our filter for congestion, throw
                    throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
                }
                handleThrottle(ex);
            }
        }
        while (!isTimedOut(startTime, timeoutMillis));

        throw new TimeoutException("Timed out before call succeeded after " + (System.currentTimeMillis() - startTime) + " ms");
    }

    public void setBlockSpiller(BlockSpiller spiller)
    {
        spillerRef.set(spiller);
    }

    public State getState()
    {
        return state;
    }

    @VisibleForTesting
    protected long getDelay()
    {
        return delay.get();
    }

    private synchronized void handleThrottle(Exception ex)
    {
        if (spillerRef.get() != null && !spillerRef.get().spilled()) {
            //If no blocks have spilled, it is better to signal the Throttle to Athena by propagating.
            throw new FederationThrottleException("ThrottlingInvoker requesting slow down due to " + ex, ex);
        }

        long newDelay = (long) Math.ceil(delay.get() / decrease);
        if (newDelay == 0) {
            newDelay = initialDelayMs;
        }
        else if (newDelay > maxDelayMs) {
            newDelay = maxDelayMs;
        }
        logger.info("handleThrottle: Encountered a Throttling event[{}] adjusting delay to {} ms @ {} TPS",
                ex, newDelay, 1000D / newDelay);
        state = State.CONGESTED;
        delay.set(newDelay);
    }

    private synchronized void handleAvoidance()
    {
        long newDelay = delay.get() - increase;
        if (newDelay <= 0) {
            newDelay = 0;
        }

        if (delay.get() > 0) {
            state = State.AVOIDANCE;
            logger.info("handleAvoidance: Congestion AVOIDANCE active, decreasing delay to {} ms @ {} TPS",
                    newDelay, (newDelay > 0) ? 1000 / newDelay : "unlimited");
            delay.set(newDelay);
        }
    }

    private void applySleep()
    {
        if (delay.get() > 0) {
            try {
                Thread.sleep(delay.get());
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
    }

    private boolean isTimedOut(long startTime, long timeoutMillis)
    {
        return (timeoutMillis > 0) ? System.currentTimeMillis() - startTime > timeoutMillis : false;
    }

    public static class Builder
    {
        private long initialDelayMs;
        private long maxDelayMs;
        private double decrease;
        private long increase;
        private ExceptionFilter filter;
        private BlockSpiller spiller;

        public Builder withInitialDelayMs(long initialDelayMs)
        {
            this.initialDelayMs = initialDelayMs;
            return this;
        }

        public Builder withMaxDelayMs(long maxDelayMs)
        {
            this.maxDelayMs = maxDelayMs;
            return this;
        }

        public Builder withDecrease(double decrease)
        {
            this.decrease = decrease;
            return this;
        }

        public Builder withIncrease(long increase)
        {
            this.increase = increase;
            return this;
        }

        public Builder withFilter(ExceptionFilter filter)
        {
            this.filter = filter;
            return this;
        }

        public Builder withSpiller(BlockSpiller spiller)
        {
            this.spiller = spiller;
            return this;
        }

        public ThrottlingInvoker build()
        {
            return new ThrottlingInvoker(this);
        }
    }
}
