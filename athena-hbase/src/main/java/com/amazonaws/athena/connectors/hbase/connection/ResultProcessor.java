/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.connection;

import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Used to define a class which is capable of processing entries returned from an HBase scan operation.
 *
 * @param <T> The type that the ResultProcessor will return.
 */
public interface ResultProcessor<T>
{
    /**
     * Used to process results from an HBase ResultScanner (aka iterator of results).
     *
     * @param scanner The iterable results from an HBase scan.
     * @return The result from the ResultProcessor.
     */
    T scan(ResultScanner scanner);
}
