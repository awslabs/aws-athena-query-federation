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
package com.amazonaws.athena.connector.lambda.data.writers.extractors;

/**
 * Used to extract a value for some context. This interface enables the use of a pseudo-code generator
 * for RowWriter which reduces object and branching overhead when translating from your source system to Apache
 * Arrow.
 * <p>
 * For example of how to use this, see ExampleRecordHandler in athena-federation-sdk.
 */
public interface Extractor {}
