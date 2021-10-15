/*-
 * #%L
 * athena-redis
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.redis.util;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScoredValueScanCursor;

import java.util.ArrayList;
import java.util.List;

public class MockScoredValueScanCursor<V> extends ScoredValueScanCursor<V> {
  private List<ScoredValue<V>> values = new ArrayList<>();

  public List<ScoredValue<V>> getValues()
  {
    return this.values;
  }

  public void setValues(List<ScoredValue<V>> values)
  {
    this.values = values;
  }
}
