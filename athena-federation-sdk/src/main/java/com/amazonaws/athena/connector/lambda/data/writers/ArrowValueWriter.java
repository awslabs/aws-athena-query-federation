package com.amazonaws.athena.connector.lambda.data.writers;

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
 * Implementation of this interface is expected to project java values into Arrow vectors. The implementation is
 * expected to take into a vector during initialization. Each call of {@link #write(int, Object) project} would
 * write one Java value into the Arrow vector.
 */
public interface ArrowValueWriter
{
    /**
     * Project the java value into Arrow's vector.
     * @param pos the position/row to project to
     * @param value the original java value to be projected
     */
    void write(int pos, Object value);
}
