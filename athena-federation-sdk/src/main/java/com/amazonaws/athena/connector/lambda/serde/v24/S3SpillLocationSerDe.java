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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

public class S3SpillLocationSerDe
        extends TypedSerDe<SpillLocation>
{
    private static final String BUCKET_FIELD = "bucket";
    private static final String KEY_FIELD = "key";
    private static final String DIRECTORY_FIELD = "directory";

    @Override
    public void doSerialize(JsonGenerator jgen, SpillLocation spillLocation)
            throws IOException
    {
        S3SpillLocation s3SpillLocation = (S3SpillLocation) spillLocation;

        jgen.writeStringField(BUCKET_FIELD, s3SpillLocation.getBucket());
        jgen.writeStringField(KEY_FIELD, s3SpillLocation.getKey());
        jgen.writeBooleanField(DIRECTORY_FIELD, s3SpillLocation.isDirectory());
    }

    @Override
    public S3SpillLocation doDeserialize(JsonParser jparser)
            throws IOException
    {
        String bucket = getNextStringField(jparser, BUCKET_FIELD);
        String key = getNextStringField(jparser, KEY_FIELD);
        boolean directory = getNextBoolField(jparser, DIRECTORY_FIELD);

        return new S3SpillLocation(bucket, key, directory);
    }
}
