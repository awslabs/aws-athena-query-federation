/*-
 * #%L
 * athena-cloudwatch-metrics
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
package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * Used to serialize and deserialize Cloudwatch Metrics Dimension objects. This is used
 * when creating and processing Splits.
 */
public class DimensionSerDe
{
    protected static final String SERIALZIE_DIM_FIELD_NAME = "d";
    private static final ObjectMapper mapper = new ObjectMapper();

    private DimensionSerDe() {}

    /**
     * Serializes the provided List of Dimensions.
     *
     * @param dim The list of dimensions to serialize.
     * @return A String containing the serialized list of Dimensions.
     */
    public static String serialize(List<Dimension> dim)
    {
        try {
            return mapper.writeValueAsString(new DimensionHolder(dim));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Deserializes the provided String into a List of Dimensions.
     *
     * @param serializeDim A serialized list of Dimensions.
     * @return The List of Dimensions represented by the serialized string.
     */
    public static List<Dimension> deserialize(String serializeDim)
    {
        try {
            return mapper.readValue(serializeDim, DimensionHolder.class).getDimensions();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Helper which allows us to use Jackson's Object Mapper to serialize a List of Dimensions.
     */
    private static class DimensionHolder
    {
        private final List<Dimension> dimensions;

        @JsonCreator
        public DimensionHolder(@JsonProperty("dimensions") List<Dimension> dimensions)
        {
            this.dimensions = dimensions;
        }

        @JsonProperty
        public List<Dimension> getDimensions()
        {
            return dimensions;
        }
    }
}
