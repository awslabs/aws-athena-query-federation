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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.lambda.invoke.LambdaFunctionNameResolver;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactoryConfig;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.amazonaws.athena.connector.validation.ConnectorValidator.BLOCK_ALLOCATOR;

/**
 * This class offers multiple convenience methods to retrieve metadata from a deployed Lambda.
 */
public class LambdaMetadataProvider
{
  private static final Logger log = LoggerFactory.getLogger(LambdaMetadataProvider.class);
  private static final String UNKNOWN_SUFFIX = "_unknown";

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
    String queryId = UUID.randomUUID().toString() + UNKNOWN_SUFFIX;
    log.info("Submitting ListSchemasRequest with ID " + queryId);

    try (ListSchemasRequest request =
                 new ListSchemasRequest(identity, queryId, catalog)) {
      log.info("Submitting request: {}", request);
      ListSchemasResponse response = (ListSchemasResponse) getService(metadataFunction).getMetadata(request);
      log.info("Received response: {}", response);
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    String queryId = UUID.randomUUID().toString() + UNKNOWN_SUFFIX;
    log.info("Submitting ListTablesRequest with ID " + queryId);

    try (ListTablesRequest request =
                 new ListTablesRequest(identity, queryId, catalog, schema)) {
      log.info("Submitting request: {}", request);
      ListTablesResponse response = (ListTablesResponse) getService(metadataFunction).getMetadata(request);
      log.info("Received response: {}", response);
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    String queryId = UUID.randomUUID().toString() + UNKNOWN_SUFFIX;
    log.info("Submitting GetTableRequest with ID " + queryId);

    try (GetTableRequest request =
                 new GetTableRequest(identity, queryId, catalog, tableName)) {
      log.info("Submitting request: {}", request);
      GetTableResponse response = (GetTableResponse) getService(metadataFunction).getMetadata(request);
      log.info("Received response: {}", response);
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    String queryId = UUID.randomUUID().toString() + UNKNOWN_SUFFIX;
    log.info("Submitting GetTableLayoutRequest with ID " + queryId);

    try (GetTableLayoutRequest request =
                 new GetTableLayoutRequest(identity, queryId, catalog, tableName, constraints, schema, partitionCols)) {
      log.info("Submitting request: {}", request);
      GetTableLayoutResponse response = (GetTableLayoutResponse) getService(metadataFunction).getMetadata(request);
      log.info("Received response: {}", response);
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    String queryId = UUID.randomUUID().toString() + UNKNOWN_SUFFIX;
    log.info("Submitting GetSplitsRequest with ID " + queryId);

    try (GetSplitsRequest request =
                 new GetSplitsRequest(identity, queryId, catalog, tableName, partitions, partitionCols, constraints, contToken)) {
      log.info("Submitting request: {}", request);
      GetSplitsResponse response = (GetSplitsResponse) getService(metadataFunction).getMetadata(request);
      log.info("Received response: {}", response);
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public interface MetadataService
  {
    @LambdaFunction
    MetadataResponse getMetadata(final MetadataRequest request);
  }

  public static final class Mapper
          implements LambdaFunctionNameResolver
  {
    private final String metadataLambda;

    private Mapper(String metadataLambda)
    {
      this.metadataLambda = metadataLambda;
    }

    @Override
    public String getFunctionName(Method method, LambdaFunction lambdaFunction,
                                  LambdaInvokerFactoryConfig lambdaInvokerFactoryConfig)
    {
      return metadataLambda;
    }
  }

  private static MetadataService getService(String lambdaFunction)
  {
    return LambdaInvokerFactory.builder()
                   .lambdaClient(AWSLambdaClientBuilder.standard().withRegion(Regions.US_EAST_2)
                                         .build())
                   .objectMapper(ObjectMapperFactory.create(BLOCK_ALLOCATOR))
                   .lambdaFunctionNameResolver(new Mapper(lambdaFunction))
                   .build(MetadataService.class);
  }
}
