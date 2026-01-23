/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.client;

import com.amazonaws.athena.connectors.deltashare.model.DeltaShareTable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HTTP client for Delta Share protocol communication. Handles discovery, metadata retrieval, and data queries.
 */
public class DeltaShareClient
{
    private static final Logger logger = LoggerFactory.getLogger(DeltaShareClient.class);
    private final String endpoint;
    private final String token;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public DeltaShareClient(String endpoint, String token)
    {
        this(endpoint, token, null);
    }

    public DeltaShareClient(String endpoint, String token, CloseableHttpClient httpClient)
    {
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("Delta Share endpoint cannot be null or empty. Please set the 'endpoint' environment variable.");
        }
        if (token == null || token.trim().isEmpty()) {
            throw new IllegalArgumentException("Delta Share token cannot be null or empty. Please set the 'token' environment variable.");
        }
        
        this.endpoint = endpoint.endsWith("/") ? endpoint : endpoint + "/";
        this.token = token;
        this.httpClient = httpClient != null ? httpClient : HttpClients.createDefault();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Lists all available shares from the Delta Share server.
     */
    public List<String> listShares() throws IOException
    {
        String url = endpoint + "shares";
        HttpGet request = new HttpGet(url);
        request.setHeader("Authorization", "Bearer " + token);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String json = EntityUtils.toString(response.getEntity());
            JsonNode root = objectMapper.readTree(json);
            JsonNode items = root.get("items");
            
            List<String> shares = new ArrayList<>();
            if (items != null && items.isArray()) {
                for (JsonNode item : items) {
                    shares.add(item.get("name").asText());
                }
            }
            return shares;
        }
    }

    /**
     * Lists all schemas in the specified share.
     */
    public List<String> listSchemas(String share) throws IOException
    {
        String url = endpoint + "shares/" + share + "/schemas";
        HttpGet request = new HttpGet(url);
        request.setHeader("Authorization", "Bearer " + token);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String json = EntityUtils.toString(response.getEntity());
            JsonNode root = objectMapper.readTree(json);
            JsonNode items = root.get("items");
            
            List<String> schemas = new ArrayList<>();
            if (items != null && items.isArray()) {
                for (JsonNode item : items) {
                    schemas.add(item.get("name").asText());
                }
            }
            return schemas;
        }
    }

    /**
     * Lists all tables in the specified schema.
     */
    public List<DeltaShareTable> listTables(String share, String schema) throws IOException
    {
        String url = endpoint + "shares/" + share + "/schemas/" + schema + "/tables";
        HttpGet request = new HttpGet(url);
        request.setHeader("Authorization", "Bearer " + token);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String json = EntityUtils.toString(response.getEntity());
            JsonNode root = objectMapper.readTree(json);
            JsonNode items = root.get("items");
            
            List<DeltaShareTable> tables = new ArrayList<>();
            if (items != null && items.isArray()) {
                for (JsonNode item : items) {
                    String name = item.get("name").asText();
                    tables.add(new DeltaShareTable(name, null));
                }
            }
            return tables;
        }
    }

    /**
     * Gets table metadata including schema and partition columns.
     */
    public JsonNode getTableMetadata(String share, String schema, String table) throws IOException
    {
        String url = endpoint + "shares/" + share + "/schemas/" + schema + "/tables/" + table + "/metadata";
        HttpGet request = new HttpGet(url);
        request.setHeader("Authorization", "Bearer " + token);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String json = EntityUtils.toString(response.getEntity());
            String[] lines = json.split("\n");
            
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                JsonNode lineNode = objectMapper.readTree(line);
                if (lineNode.has("metaData")) {
                    JsonNode metaData = lineNode.get("metaData");
                    
                    JsonNode tableSchema = objectMapper.readTree(metaData.get("schemaString").asText());
                    
                    ObjectNode result = objectMapper.createObjectNode();
                    
                    if (tableSchema.has("fields")) {
                        result.set("fields", tableSchema.get("fields"));
                    }
                    if (tableSchema.has("type")) {
                        result.set("type", tableSchema.get("type"));
                    }
                    
                    if (metaData.has("partitionColumns")) {
                        result.set("partitionColumns", metaData.get("partitionColumns"));
                    } else {
                        result.set("partitionColumns", objectMapper.createArrayNode());
                    }
                    
                    if (metaData.has("id")) {
                        result.set("id", metaData.get("id"));
                    }
                    if (metaData.has("format")) {
                        result.set("format", metaData.get("format"));
                    }
                    return result;
                }
            }
            return null;
        }
    }

    /**
     * Queries a table to get file references for data processing.
     */
    public JsonNode queryTable(String share, String schema, String table) throws IOException
    {
        String url = endpoint + "shares/" + share + "/schemas/" + schema + "/tables/" + table + "/query";
        HttpPost request = new HttpPost(url);
        request.setHeader("Authorization", "Bearer " + token);
        request.setHeader("Content-Type", "application/json");
        
        String requestBody = String.format("{\"sql\": \"SELECT * FROM \\\"%s\\\".\\\"%s\\\"\"}", schema, table);
        request.setEntity(new StringEntity(requestBody));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String json = EntityUtils.toString(response.getEntity());
            String[] lines = json.split("\n");
            
            ArrayNode result = objectMapper.createArrayNode();
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                JsonNode lineNode = objectMapper.readTree(line);
                result.add(lineNode);
            }
            return result;
        }
    }

    /**
     * Queries a table with predicate hints for server-side filtering.
     */
    public JsonNode queryTableWithPredicateHints(String share, String schema, String table, String jsonPredicateHints) throws IOException
    {
        String url = endpoint + "shares/" + share + "/schemas/" + schema + "/tables/" + table + "/query";
        HttpPost request = new HttpPost(url);
        request.setHeader("Authorization", "Bearer " + token);
        request.setHeader("Content-Type", "application/json");
        
        ObjectNode requestBody = objectMapper.createObjectNode();
        
        requestBody.put("jsonPredicateHints", jsonPredicateHints);
        
        requestBody.put("limitHint", 1000);
        
        request.setEntity(new StringEntity(requestBody.toString()));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            String json = EntityUtils.toString(response.getEntity());
            
            if (statusCode != 200) {
                logger.error("Query failed with status {}: {}", statusCode, json);
                throw new IOException("Delta Share query failed with status " + statusCode + ": " + json);
            }
            
            String[] lines = json.split("\n");
            
            ArrayNode result = objectMapper.createArrayNode();
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                try {
                    JsonNode lineNode = objectMapper.readTree(line);
                    result.add(lineNode);
                } catch (Exception e) {
                    logger.warn("Failed to parse response line: {}", line, e);
                }
            }
            
            return result;
        }
    }

    /**
     * Queries a table with custom parameters.
     */
    public JsonNode queryTableWithCustomParams(String share, String schema, String table, Map<String, Object> queryParams) throws IOException
    {
        String url = endpoint + "shares/" + share + "/schemas/" + schema + "/tables/" + table + "/query";
        HttpPost request = new HttpPost(url);
        request.setHeader("Authorization", "Bearer " + token);
        request.setHeader("Content-Type", "application/json");
        
        String requestBody = objectMapper.writeValueAsString(queryParams);
        request.setEntity(new StringEntity(requestBody));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            String json = EntityUtils.toString(response.getEntity());
            
            if (statusCode != 200) {
                logger.error("Query failed with status {}: {}", statusCode, json);
                throw new IOException("Delta Share query failed with status " + statusCode + ": " + json);
            }
            
            String[] lines = json.split("\n");
            
            ArrayNode result = objectMapper.createArrayNode();
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                JsonNode lineNode = objectMapper.readTree(line);
                result.add(lineNode);
            }
            return result;
        }
    }

    /**
     * Executes a generic POST query to Delta Share endpoint.
     */
    public <T> T postQuery(String url, String requestBody, Class<T> responseType) throws IOException
    {
        HttpPost request = new HttpPost(url);
        request.setHeader("Authorization", "Bearer " + token);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(requestBody));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String json = EntityUtils.toString(response.getEntity());
            return objectMapper.readValue(json, responseType);
        }
    }
    
    /**
     * Returns the configured endpoint URL.
     */
    public String getEndpoint()
    {
        return endpoint;
    }

    /**
     * Closes the HTTP client and releases resources.
     */
    public void close() throws IOException
    {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
