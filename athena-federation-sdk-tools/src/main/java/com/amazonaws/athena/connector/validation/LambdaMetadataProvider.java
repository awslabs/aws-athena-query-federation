/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.validation;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.validation.FederationServiceProvider.generateQueryId;
import static com.amazonaws.athena.connector.validation.FederationServiceProvider.getService;

/**
 * This class offers multiple convenience methods to retrieve metadata from a deployed Lambda.
 */
public class LambdaMetadataProvider
{
  private static final Logger log = LoggerFactory.getLogger(LambdaMetadataProvider.class);

  private LambdaMetadataProvider()
  {
    // Intentionally left blank.
  }

  /**
   * This method builds and executes a ListSchemasRequest against the specified Lambda function.
   *
   * @param catalog the catalog name to be passed to Lambda
   * @param metadataFunction the name of the Lambda function to call
   * @param identity the identity of the caller
   * @return the response
   */
  public static ListSchemasResponse listSchemas(String catalog,
                                         String metadataFunction,
                                         FederatedIdentity identity)
  {
    String queryId = generateQueryId();
    log.info("Submitting ListSchemasRequest with ID " + queryId);

    ListSchemasRequest request =
        ListSchemasRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).build();
    log.info("Submitting request: {}", request);
    ListSchemasResponse response = (ListSchemasResponse) getService(metadataFunction, identity, catalog).call(request);
    log.info("Received response: {}", response);
    return response;
  }

  /**
   * This method builds and executes a ListTablesRequest against the specified Lambda function.
   *
   * @param catalog the catalog name to be passed to Lambda
   * @param schema the name of the contextual schema for the request
   * @param metadataFunction the name of the Lambda function to call
   * @param identity the identity of the caller
   * @return the response
   */
  public static ListTablesResponse listTables(String catalog,
                                         String schema,
                                         String metadataFunction,
                                         FederatedIdentity identity)
  {
    String queryId = generateQueryId();
    log.info("Submitting ListTablesRequest with ID " + queryId);

    /**
     * TODO: Add logic to ensure that the connector supports pagination.
     */
    ListTablesRequest request = ListTablesRequest.newBuilder()
        .setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setSchemaName(schema).setPageSize(UNLIMITED_PAGE_SIZE_VALUE).build();
    log.info("Submitting request: {}", request);
    ListTablesResponse response = (ListTablesResponse) getService(metadataFunction, identity, catalog).call(request);
    log.info("Received response: {}", response);
    return response;
  }

  /**
   * This method builds and executes a GetTableRequest against the specified Lambda function.
   *
   * @param catalog the catalog name to be passed to Lambda
   * @param tableName the schema-qualified table name indicating which table should be retrieved
   * @param metadataFunction the name of the Lambda function to call
   * @param identity the identity of the caller
   * @return the response
   */
  public static GetTableResponse getTable(String catalog,
                                       TableName tableName,
                                       String metadataFunction,
                                       FederatedIdentity identity)
  {
    String queryId = generateQueryId();
    log.info("Submitting GetTableRequest with ID " + queryId);

    GetTableRequest request =
        GetTableRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setTableName(tableName).build();
      log.info("Submitting request: {}", request);
      GetTableResponse response = (GetTableResponse) getService(metadataFunction, identity, catalog).call(request);
      log.info("Received response: {}", response);
      return response;
  }

  /**
   * This method builds and executes a GetTableLayoutRequest against the specified Lambda function.
   *
   * @param catalog the catalog name to be passed to Lambda
   * @param tableName the schema-qualified table name indicating the table whose layout should be retrieved
   * @param constraints the constraints to be applied to the request
   * @param schema the schema of the table in question
   * @param partitionCols the partition column names for the table in question
   * @param metadataFunction the name of the Lambda function to call
   * @param identity the identity of the caller
   * @return the response
   */
  public static GetTableLayoutResponse getTableLayout(String catalog,
                                     TableName tableName,
                                     Constraints constraints,
                                     Schema schema,
                                     Set<String> partitionCols,
                                     String metadataFunction,
                                     FederatedIdentity identity)
  {
    String queryId = generateQueryId();
    log.info("Submitting GetTableLayoutRequest with ID " + queryId);

    GetTableLayoutRequest request =
                GetTableLayoutRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setTableName(tableName).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schema)).addAllPartitionColumns(partitionCols).build();
    log.info("Submitting request: {}", request);
    GetTableLayoutResponse response = (GetTableLayoutResponse) getService(metadataFunction, identity, catalog).call(request);
    log.info("Received response: {}", response);
    return response;
  }

  /**
   * This method builds and executes a GetSplitsRequest against the specified Lambda function.
   *
   * @param catalog the catalog name to be passed to Lambda
   * @param tableName the schema-qualified table name indicating the table for which splits should be retrieved
   * @param constraints the constraints to be applied to the request
   * @param partitions the block of partitions to be provided with the request
   * @param partitionCols the partition column names for the table in question
   * @param contToken a continuation token to be provided with the request, or null
   * @param metadataFunction the name of the Lambda function to call
   * @param identity the identity of the caller
   * @return the response
   */
  public static GetSplitsResponse getSplits(String catalog,
                                            TableName tableName,
                                            Constraints constraints,
                                            Block partitions,
                                            List<String> partitionCols,
                                            String contToken,
                                            String metadataFunction,
                                            FederatedIdentity identity)
  {
    String queryId = generateQueryId();
    log.info("Submitting GetSplitsRequest with ID " + queryId);

    GetSplitsRequest request =
        GetSplitsRequest.newBuilder().setIdentity(identity).setQueryId(queryId).setCatalogName(catalog).setTableName(tableName).setPartitions(ProtobufMessageConverter.toProtoBlock(partitions)).addAllPartitionColumns(partitionCols).setConstraints(ProtobufMessageConverter.toProtoConstraints(constraints)).setContinuationToken(contToken).build();
    log.info("Submitting request: {}", request);
    GetSplitsResponse response = (GetSplitsResponse) getService(metadataFunction, identity, catalog).call(request);
    log.info("Received response: {}", response);
    return response;
    
  }
}
