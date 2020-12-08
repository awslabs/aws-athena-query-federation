/*-
 * #%L
 *athena-slack-member-analytics
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
package com.amazonaws.connectors.athena.slack.util;

import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.connectors.athena.slack.util.SlackHttpUtility;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import org.json.JSONArray;
import java.io.BufferedReader;
import java.util.Base64;
import java.util.HashMap;
import java.time.LocalDate;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;

public class SlackSchemaUtility {
    
    private static final Logger logger          = LoggerFactory.getLogger(SlackSchemaUtility.class);
    private static final String TYPE_INTEGER    = "int";
    private static final String TYPE_STRING     = "string";
    private static final String TYPE_FLOAT4     = "float";
    private static final String TYPE_STRUCT     = "struct";
    private static final String TYPE_LIST       = "list";
    
    /**
     * Retrieves json schema for a particular table.
     * 
     * @param tableName  String with the name of the table.
     * @return JSONObject with the table schema
     * 
     */
    public static JSONObject getSchema(String tableName) {
        
        logger.info("getSchema: enter - " + tableName);
    
        // The slack member analytics has only one endpoint/table
        JSONObject data = getMasterRecord(1);
        // Try sample from two days ago if is empty.
        if (data.length()==0)
            data = getMasterRecord(2);
        
        logger.info("getSchema: exit");
        return processSchema(data, false);
    }
    
    /**
     * Help function to extract metadata based on the first 20 records. 
     * 
     * @param reader BufferedReader with sample records.
     * @return JSONObject with master record.
     */
    private static JSONObject getMasterRecord(int minusDays){
        
        logger.info("getMasterRecord - enter - try " + minusDays);    
        JSONObject master = new JSONObject();
        
        try {
            
            //Get sample records from the Slack Members API to extract metadata.
            String yesterday = LocalDate.now().minusDays(minusDays).toString();
            logger.info("getSchema: Extracting metadata from {} record sample", yesterday);
            String baseURL = System.getenv("data_endpoint");
            URIBuilder requestURI  = new URIBuilder(baseURL);
            requestURI.addParameter("date", yesterday);
            requestURI.addParameter("type", "member");
            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("Authorization", "Bearer " + getSlackToken());
            BufferedReader reader = SlackHttpUtility.getData(requestURI, headers);    
            
            String line;
            int counter = 0;
            
            while (reader != null && (line = reader.readLine()) != null) { // Read line by line
                logger.debug("getMasterRecord: Line - " + line);
                JSONObject record = new JSONObject(line);
                
                // Adding field to master if not exists
                // For member analytics we don't expect a nested structure.
                for (String keyStr : record.keySet()) {
                    if(!master.has(keyStr))
                        master.put(keyStr, record.get(keyStr));
                }
                
                counter++;
                if (counter==20) break;
            }
        } catch (Exception e) {
            logger.error("getMasterRecord: Error while extracting schema. {}", e.getMessage());
            master.put("INVALID_SCHEMA","EMPTY");
        }
        
        logger.info("getMasterRecord - exit - Record: {}", master.toString());
        return master;
    }
    
    /**
     * Help function that loops through the sample data and identifies
     * column types. 
     * 
     * @param data JSONObject with sample data record
     * @param innerLoop boolean to describe weather this is a recursive call.
     * @return JSONObject schema definition in json.
     */
    private static JSONObject processSchema(JSONObject data, Boolean isInnerLoop) {
        logger.info("processSchema: enter - Parsing through json schema");
        JSONObject schema = new JSONObject();
        for (String key : data.keySet()){
            JSONObject def = new JSONObject();
            // TODO - Handle nested data types (list, Structs). Not required for Slack Member Analytics
            if(data.get(key) instanceof JSONObject){
                def.put("type", TYPE_STRUCT);
                def.put("items", processSchema(data.getJSONObject(key), true));
            } else if (data.get(key) instanceof JSONArray){
                def.put("type", TYPE_LIST);
                def.put("items", data.get(key));
            } else if (data.get(key) instanceof Double){
                def.put("type", TYPE_FLOAT4);
            } else if(data.get(key) instanceof Integer){
                def.put("type", TYPE_INTEGER);
            } else {
                def.put("type", TYPE_STRING);
            }
            schema.put(key, def);
        }
        
        if(!isInnerLoop){
            logger.info("processSchema: Schema={}", schema.toString());
        }
        logger.info("processSchema: exit");
        return schema;
    }
    
    /**
     * Help function to build SchemaBuilder from json schema. The current
     * implementation does not support nested schemas (struct/arrays).
     * 
     * @param tableName  String with the name of the table.
     * @return SchemaBuilder object. 
     * 
     */
    public static SchemaBuilder getSchemaBuilder(String tableName){
        
        logger.info("getSchemaBuilder: enter");
        SchemaBuilder result    = SchemaBuilder.newBuilder();
        JSONObject schema       = getSchema(tableName);
        for (String key : schema.keySet()){
            // TODO - Handle nested json (struct, list). 
            // For slack member analytics endpoint, we don't expect addiitonal data types.
            switch(schema.getJSONObject(key).getString("type")) {
                case TYPE_INTEGER:
                    result.addIntField(key);
                    break;
                case TYPE_FLOAT4:
                    result.addFloat4Field(key);
                    break;
                default:
                    result.addStringField(key);
            }
        }

        logger.info("getSchemaBuilder: exit");
        return result;
    }
    
    public static GeneratedRowWriter.RowWriterBuilder getRowWriterBuilder(ReadRecordsRequest recordsRequest, String tableName) {
        logger.info("getRowWriterBuilder: enter");
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
         
        logger.info("getRowWriterBuilder: Parsing through json schema"); 
        JSONObject schema = getSchema(tableName);
        for (String key : schema.keySet()){
            // TODO - Handle decimal values,  structs and lists. 
            // For slack memeber analytics endpoint we only expect integers and strings
            switch (schema.getJSONObject(key).getString("type")) {
                case TYPE_INTEGER:
                    logger.debug("getRowWriterBuilder: key={}, type=Integer", key);
                    builder.withExtractor(key, (IntExtractor) (Object context, NullableIntHolder value) -> {
                        value.isSet = 1;
                        value.value = 0;
                        if(((JSONObject) context).has(key)) 
                            value.value = ((JSONObject) context).optInt(key);
                    });
                    break;
                default:
                    logger.debug("getRowWriterBuilder: key={}, type=String", key);
                    builder.withExtractor(key, (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
                        value.isSet = 1;
                        value.value = "";
                        if(((JSONObject) context).has(key))
                            value.value = ((JSONObject) context).optString(key);
                    }); 
            }
        }
        logger.info("getRowWriterBuilder: exit");
        return builder;
    }

    /**
     * Use to get the slack token from AWS Secrets manager
     *
     * If you need more information about configurations or implementing the
     * sample code, visit the AWS docs:
     * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/java-dg-samples.html#prerequisites
     *
     * @return String Slack authentiation token
     * @throws Exception if unable to get secret.
     **/
    public static String getSlackToken()
            throws Exception {
        logger.info("getSlackToken: enter");

        String secretName = System.getenv("secret_name");
        String region = System.getenv("region");

        if (secretName==null || secretName.isEmpty() || region==null || region.isEmpty())
            throw new Exception("Missing AWS Secrets environment variables.");

        logger.info("getSlackToken: Retrieving " + secretName);

        // Create a Secrets Manager client
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        String secret, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = client.getSecretValue(getSecretValueRequest);

        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        }
        else {
            secret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
        }

        JSONObject slackSecret = new JSONObject(secret);

        String slackToken = "";
        if(slackSecret.has("access_token"))
            slackToken = slackSecret.getString("access_token");

        logger.info("getSlackToken: exit");

        return slackToken;

    }
    
}
    

