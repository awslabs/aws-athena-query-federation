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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
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
import java.util.UUID;

import static com.amazonaws.athena.connector.validation.ConnectorValidator.BLOCK_ALLOCATOR;

/**
 * This class offers a convenience method to retrieve records from a deployed Lambda.
 */
public class LambdaRecordProvider
{
  private static final Logger log = LoggerFactory.getLogger(LambdaMetadataProvider.class);
  
  private static final long MAX_BLOCK_SIZE = 16000000;
  private static final long MAX_INLINE_BLOCK_SIZE = 5242880;

  private LambdaRecordProvider()
  {
    // Intentionally left blank.
  }

  /**
   * This method builds and executes a ReadRecordsRequest against the specified Lambda function.
   *
   * @param catalog the catalog name to be passed to Lambda
   * @param tableName the schema-qualified table name indicating the table for which splits should be retrieved
   * @param constraints the constraints to be applied to the request
   * @param schema the schema of the table in question
   * @param split the split to be read in this request
   * @param recordFunction the name of the Lambda function to call
   * @param identity the identity of the caller
   * @return the response
   */
  public static ReadRecordsResponse readRecords(String catalog,
                                                TableName tableName,
                                                Constraints constraints,
                                                Schema schema,
                                                Split split,
                                                String recordFunction,
                                                FederatedIdentity identity)
  {
    String queryId = UUID.randomUUID().toString() + "_unknown";
    log.info("Submitting ReadRecordsRequest with ID " + queryId);

    try (ReadRecordsRequest request =
                 new ReadRecordsRequest(identity,
                                        queryId,
                                        catalog,
                                        tableName,
                                        schema,
                                        split,
                                        constraints,
                                        MAX_BLOCK_SIZE,
                                        MAX_INLINE_BLOCK_SIZE)) {
      log.info("Submitting request: {}", request);
      ReadRecordsResponse response = (ReadRecordsResponse) getService(recordFunction).readRecords(request);
      log.info("Received response: {}", response);
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public interface RecordService
  {
    @LambdaFunction
    RecordResponse readRecords(final RecordRequest request);
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

  private static RecordService getService(String lambdaFunction)
  {
    return LambdaInvokerFactory.builder()
                   .lambdaClient(AWSLambdaClientBuilder.standard().withRegion(Regions.US_EAST_2)
                                         .build())
                   .objectMapper(ObjectMapperFactory.create(BLOCK_ALLOCATOR))
                   .lambdaFunctionNameResolver(new Mapper(lambdaFunction))
                   .build(RecordService.class);
  }
}
