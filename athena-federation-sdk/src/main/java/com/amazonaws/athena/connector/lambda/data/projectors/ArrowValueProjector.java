package com.amazonaws.athena.connector.lambda.data.projectors;

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
 * Implementation of this interface is expected to project Arrow data into Java objects. The implementation is
 * expected to take into a fieldReader during initialization. Each call of {@link #project(int) project} would
 * project one Arrow datum to a Java Object the object.
 */
public interface ArrowValueProjector
{
    /**
     * Projects Arrow datum into a matching Java object
     * @param pos the position/row to project to
     * @return The corresponding Java object matching the Arrow datum.
     */
    Object project(int pos);
}
