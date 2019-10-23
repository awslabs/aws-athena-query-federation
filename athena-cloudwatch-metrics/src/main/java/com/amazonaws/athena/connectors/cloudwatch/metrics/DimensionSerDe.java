package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

public class DimensionSerDe
{
    protected static final String SERIALZIE_DIM_FIELD_NAME = "d";
    private static final ObjectMapper mapper = new ObjectMapper();

    private DimensionSerDe() {}

    public static String serialize(List<Dimension> dim)
    {
        try {
            return mapper.writeValueAsString(new DimensionHolder(dim));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static List<Dimension> deserialize(String serializeDim)
    {
        try {
            return mapper.readValue(serializeDim, DimensionHolder.class).getDimensions();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

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
