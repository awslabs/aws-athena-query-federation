/*-
 * #%L
 * athena-msk
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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.msk.dto.MSKField;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class GlueRegistryReader
{
    private static final Logger logger = LoggerFactory.getLogger(GlueRegistryReader.class);
    private static final ObjectMapper objectMapper;
    private static final String PROTO_FILE = "schema.proto";
    private static final String DESC_FILE = "schema.desc";

    static {
        objectMapper = new ObjectMapper();
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Parse protobuf schema definition from Glue Schema Registry using protoc compiler
     * @param glueRegistryName Registry name
     * @param glueSchemaName Schema name
     * @return List of MSKField objects containing field information
     * @throws AthenaConnectorException if schema parsing fails
     */
    public List<MSKField> getProtobufFields(String glueRegistryName, String glueSchemaName)
    {
        // Get schema from Glue
        GetSchemaVersionResponse schemaVersionResponse = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        String schemaDef = schemaVersionResponse.schemaDefinition();

        // Create a unique temp directory using UUID
        Path protoDir = Paths.get("/tmp", "proto_" + UUID.randomUUID());
        Path protoFile = protoDir.resolve(PROTO_FILE);
        Path descFile = protoDir.resolve(DESC_FILE);

        try {
            Files.createDirectories(protoDir);
            Files.writeString(protoFile, schemaDef);
            // Compile using protoc-jar
            int exitCode = Protoc.runProtoc(new String[]{
                    "--descriptor_set_out=" + descFile.toAbsolutePath(),
                    "--proto_path=" + protoDir.toAbsolutePath(),
                    protoFile.getFileName().toString()
            });

            if (exitCode != 0 || !Files.exists(descFile)) {
                throw new AthenaConnectorException(
                        "Failed to generate descriptor set with protoc",
                        ErrorDetails.builder()
                                .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                                .build()
                );
            }

            try (FileInputStream fis = new FileInputStream(descFile.toFile())) {
                FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(fis);

                if (descriptorSet.getFileList().isEmpty() ||
                        descriptorSet.getFile(0).getMessageTypeList().isEmpty()) {
                    throw new AthenaConnectorException(
                            "No message types found in compiled schema",
                            ErrorDetails.builder()
                                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                                    .build()
                    );
                }

                List<MSKField> fields = new ArrayList<>();
                DescriptorProto messageType = descriptorSet.getFile(0).getMessageType(0);
                for (FieldDescriptorProto field : messageType.getFieldList()) {
                    String fieldType = getFieldTypeString(field);
                    fields.add(new MSKField(field.getName(), fieldType));
                }

                return fields;
            }
        }
        catch (IOException | InterruptedException e) {
            throw new AthenaConnectorException(
                    "Error while handling schema files or protoc execution",
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                            .build()
            );
        }
        finally {
            // Clean up temporary files
            try {
                Files.deleteIfExists(protoFile);
                Files.deleteIfExists(descFile);
                Files.deleteIfExists(protoDir);
            }
            catch (IOException e) {
                logger.warn("Failed to clean up temporary proto directory: {}", protoDir.toAbsolutePath(), e);
            }
        }
    }

    /**
     * Convert protobuf field type to string representation
     */
    private String getFieldTypeString(FieldDescriptorProto field)
    {
        String baseType = field.getType().toString().toLowerCase().replace("type_", "");
        return field.getLabel() == FieldDescriptorProto.Label.LABEL_REPEATED ? 
               "repeated " + baseType : baseType;
    }

    /**
     * Fetch glue schema content for latest version
     * @param glueRegistryName
     * @param glueSchemaName
     * @return
     */
    public GetSchemaVersionResponse getSchemaVersionResult(String glueRegistryName, String glueSchemaName)
    {
        GlueClient glue = GlueClient.create();
        SchemaId sid = SchemaId.builder()
                .registryName(glueRegistryName)
                .schemaName(glueSchemaName)
                .build();
        GetSchemaResponse schemaResult = glue.getSchema(GetSchemaRequest.builder().schemaId(sid).build());
        SchemaVersionNumber svn = SchemaVersionNumber.builder()
                .versionNumber(schemaResult.latestSchemaVersion())
                .build();
        return glue.getSchemaVersion(GetSchemaVersionRequest.builder()
                .schemaId(sid)
                .schemaVersionNumber(svn)
                .build()
        );
    }

    /**
     * fetch schema file content from glue schema.
     *
     * @param glueRegistryName
     * @param glueSchemaName
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> T getGlueSchema(String glueRegistryName, String glueSchemaName, Class<T> clazz) throws Exception
    {
        GetSchemaVersionResponse result = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        return objectMapper.readValue(result.schemaDefinition(), clazz);
    }

    public String getGlueSchemaType(String glueRegistryName, String glueSchemaName)
    {
        GetSchemaVersionResponse result = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        return result.dataFormatAsString();
    }
}
