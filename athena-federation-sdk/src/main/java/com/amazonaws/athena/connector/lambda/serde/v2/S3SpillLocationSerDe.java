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

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public final class S3SpillLocationSerDe
{
    private static final String BUCKET_FIELD = "bucket";
    private static final String KEY_FIELD = "key";
    private static final String DIRECTORY_FIELD = "directory";

    private S3SpillLocationSerDe(){}

    public static final class Serializer extends TypedSerializer<SpillLocation>
    {
        public Serializer()
        {
            super(SpillLocation.class, S3SpillLocation.class);
        }

        @Override
        protected void doTypedSerialize(SpillLocation spillLocation, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            S3SpillLocation s3SpillLocation = (S3SpillLocation) spillLocation;

            jgen.writeStringField(BUCKET_FIELD, s3SpillLocation.getBucket());
            jgen.writeStringField(KEY_FIELD, s3SpillLocation.getKey());
            jgen.writeBooleanField(DIRECTORY_FIELD, s3SpillLocation.isDirectory());
        }
    }

    public static final class Deserializer extends TypedDeserializer<SpillLocation>
    {
        public Deserializer()
        {
            super(SpillLocation.class, S3SpillLocation.class);
        }

        @Override
        protected SpillLocation doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String bucket = getNextStringField(jparser, BUCKET_FIELD);
            String key = getNextStringField(jparser, KEY_FIELD);
            boolean directory = getNextBoolField(jparser, DIRECTORY_FIELD);

            return new S3SpillLocation(bucket, key, directory);
        }
    }
}
