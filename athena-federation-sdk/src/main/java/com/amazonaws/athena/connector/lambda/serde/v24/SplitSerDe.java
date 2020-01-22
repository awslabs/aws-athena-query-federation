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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.serde.BaseSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SplitSerDe extends BaseSerDe<Split>
{
    private static final String SPILL_LOCATION_FIELD = "spillLocation";
    private static final String ENCRYPTION_KEY_FIELD = "encryptionKey";
    private static final String PROPERTIES_FIELD = "properties";

    private final SpillLocationSerDe spillLocationSerDe;
    private final EncryptionKeySerDe encryptionKeySerDe;

    public SplitSerDe(SpillLocationSerDe spillLocationSerDe, EncryptionKeySerDe encryptionKeySerDe)
    {
        this.spillLocationSerDe = requireNonNull(spillLocationSerDe, "s3SpillLocationSerDe is null");
        this.encryptionKeySerDe = requireNonNull(encryptionKeySerDe, "encryptionKeySerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, Split split)
            throws IOException
    {
        jgen.writeFieldName(SPILL_LOCATION_FIELD);
        spillLocationSerDe.serialize(jgen, split.getSpillLocation());

        jgen.writeFieldName(ENCRYPTION_KEY_FIELD);
        if (split.getEncryptionKey() != null) {
            encryptionKeySerDe.serialize(jgen, split.getEncryptionKey());
        }
        else {
            jgen.writeNull();
        }

        jgen.writeObjectFieldStart(PROPERTIES_FIELD);
        for (Map.Entry<String, String> entry : split.getProperties().entrySet()) {
            jgen.writeFieldName(entry.getKey());
            jgen.writeString(entry.getValue());
        }
        jgen.writeEndObject();
    }

    @Override
    public Split doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, SPILL_LOCATION_FIELD);
        SpillLocation spillLocation = spillLocationSerDe.deserialize(jparser);

        assertFieldName(jparser, ENCRYPTION_KEY_FIELD);
        EncryptionKey encryptionKey = encryptionKeySerDe.deserialize(jparser);

        assertFieldName(jparser, PROPERTIES_FIELD);
        validateObjectStart(jparser.nextToken());
        ImmutableMap.Builder<String, String> propertiesMap = ImmutableMap.builder();
        while (jparser.nextToken() != JsonToken.END_OBJECT) {
            String key = jparser.getValueAsString();
            jparser.nextToken();
            String value = jparser.getValueAsString();
            propertiesMap.put(key, value);
        }

        return new Split(spillLocation, encryptionKey, propertiesMap.build());
    }
}
