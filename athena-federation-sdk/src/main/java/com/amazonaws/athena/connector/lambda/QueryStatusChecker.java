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
package com.amazonaws.athena.connector.lambda;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.InvalidRequestException;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * This class provides a mechanism for callers to terminate in-progress work if the upstream Athena query waiting for that work has
 * already terminated.  Callers using the SDK as-is should only need to call #isQueryRunning, as #startQueryStatusChecker
 * should have already been called by {@link com.amazonaws.athena.connector.lambda.handlers.MetadataHandler} or
 * {@link com.amazonaws.athena.connector.lambda.handlers.RecordHandler}.
 */
public class QueryStatusChecker
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(QueryStatusChecker.class);

    // progressively longer delays at which to poll
    private static final int[] FIBONACCI = new int[] { 1, 1, 2, 3, 5, 8, 13, 21, 34, 55};
    // Athena terminal states
    private static final Set<String> TERMINAL_STATES = ImmutableSet.of("SUCCEEDED", "FAILED", "CANCELLED");

    private boolean wasStarted = false;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AmazonAthena athena;
    private final ThrottlingInvoker athenaInvoker;
    private final String queryId;
    private final Thread checkerThread;

    public QueryStatusChecker(AmazonAthena athena, ThrottlingInvoker athenaInvoker, String queryId)
    {
        this.athena = athena;
        this.athenaInvoker = athenaInvoker;
        this.queryId = queryId;
        this.checkerThread = new Thread(() -> runQueryStatusChecker(queryId), "QueryStatusCheckerThread-" + queryId);
    }

    /**
     * Returns whether the query is still running
     */
    public boolean isQueryRunning()
    {
        // start the checker thread if it hasn't started already
        if (!wasStarted) {
            synchronized (this) {
                if (!wasStarted) {
                    checkerThread.start();
                    wasStarted = true;
                }
            }
        }
        return isRunning.get();
    }

    /**
     * Stops the status checker thread
     */
    @Override
    public void close()
    {
        // fine if the thread isn't running
        checkerThread.interrupt();
        logger.debug("Interrupt signal sent to status checker thread");
    }

    private void runQueryStatusChecker(String queryId)
    {
        int attempt = 0;
        while (isRunning.get()) {
            int delay = FIBONACCI[Math.min(attempt, FIBONACCI.length - 1)];
            try {
                Thread.sleep(delay * 1000);
                checkStatus(queryId, attempt);
            }
            catch (InterruptedException e) {
                logger.debug("Checker thread interrupted. Ceasing status polling");
                return;
            }
            attempt++;
        }
        logger.debug("Query terminated. Ceasing status polling");
    }

    private void checkStatus(String queryId, int attempt)
            throws InterruptedException
    {
        logger.debug(format("Background thread checking status of Athena query %s, attempt %d", queryId, attempt));
        try {
            GetQueryExecutionResult queryExecution = athenaInvoker.invoke(() -> athena.getQueryExecution(new GetQueryExecutionRequest().withQueryExecutionId(queryId)));
            String state = queryExecution.getQueryExecution().getStatus().getState();
            if (TERMINAL_STATES.contains(state)) {
                logger.debug("Query {} has terminated with state {}", queryId, state);
                isRunning.set(false);
            }
        }
        catch (Exception e) {
            logger.warn("Exception {} thrown when calling Athena for query status: {}", e.getClass().getSimpleName(), e.getMessage());
            if (e instanceof InvalidRequestException) {
                // query does not exist, so no need to keep calling Athena
                logger.debug("Athena reports query {} not found. Interrupting checker thread", queryId);
                throw new InterruptedException();
            }
        }
    }
}
