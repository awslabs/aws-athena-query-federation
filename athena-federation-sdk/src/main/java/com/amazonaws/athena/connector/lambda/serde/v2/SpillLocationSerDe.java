/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.serde.DelegatingDeserializer;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerializer;
import com.google.common.collect.ImmutableSet;

public final class SpillLocationSerDe
{
    private SpillLocationSerDe(){}

    public static final class Serializer extends DelegatingSerializer<SpillLocation>
    {
        public Serializer(S3SpillLocationSerDe.Serializer s3SpillLocationSerializer)
        {
            super(SpillLocation.class, ImmutableSet.of(s3SpillLocationSerializer));
        }
    }

    public static final class Deserializer extends DelegatingDeserializer<SpillLocation>
    {
        public Deserializer(S3SpillLocationSerDe.Deserializer s3SpillLocationDeserializer)
        {
            super(SpillLocation.class, ImmutableSet.of(s3SpillLocationDeserializer));
        }
    }
}
