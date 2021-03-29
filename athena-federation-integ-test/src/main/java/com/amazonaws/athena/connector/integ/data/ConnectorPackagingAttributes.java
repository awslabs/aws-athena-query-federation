/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
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
package com.amazonaws.athena.connector.integ.data;

/**
 * The Lambda's executable is packaged in an S3 bucket ready to be deployed by the CloudFormation stack (a result of
 * running `sam package` - see README). This class contains the attributes needed by CloudFormation to find and deploy
 * the Lambda function.
 * s3Bucket - The S3 bucket where the packaged Lambda resides.
 * s3Key - The folder within the s3Bucket.
 * lambdaFunctionHandler - the handler class for the Lambda (e.g. com.amazonaws.athena.connectors.dynamodb.DynamoDBCompositeHandler).
 */
public class ConnectorPackagingAttributes
{
    private final String s3Bucket;
    private final String s3Key;
    private final String lambdaFunctionHandler;

    public ConnectorPackagingAttributes(String s3Bucket, String s3Key, String lambdaFunctionHandler)
    {
        this.s3Bucket = s3Bucket;
        this.s3Key = s3Key;
        this.lambdaFunctionHandler = lambdaFunctionHandler;
    }

    /**
     * Public accessor for the Connector's S3 bucket.
     * @return Connector's S3 bucket
     */
    public String getS3Bucket()
    {
        return s3Bucket;
    }

    /**
     * Public accessor for the location of the connector's artifact in the spill bucket (S3).
     * @return Artifact's S3 Key
     */
    public String getS3Key()
    {
        return s3Key;
    }

    /**
     * Public accessor for the Connector's handler.
     * @return Connector's handler
     */
    public String getLambdaFunctionHandler()
    {
        return lambdaFunctionHandler;
    }
}
