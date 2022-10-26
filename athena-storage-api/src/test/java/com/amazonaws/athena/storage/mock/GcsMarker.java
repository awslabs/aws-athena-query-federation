/*-
 * #%L
 * athena-storage-api
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.storage.mock;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;

public class GcsMarker extends Marker
{

    private Object value;

    public GcsMarker(Block block, Bound bound, boolean nullValue)
    {
        super(block, bound, nullValue);
    }

    @Override
    public synchronized Object getValue()
    {
        if (isNullValue()) {
            throw new IllegalStateException("No value to get");
        }
        return this.value;
//        return FilterContext.getInstance().value(new FilterContext.MarkerKey(getValueBlock(), getBound()));
    }

    public GcsMarker withValue(Object value)
    {
        this.value = value;
        return this;
    }
}
