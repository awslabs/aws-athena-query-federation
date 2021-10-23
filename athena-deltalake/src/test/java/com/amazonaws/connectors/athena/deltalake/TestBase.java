/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import io.findify.s3mock.response.Bucket;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class TestBase {

    private static final Logger logger = LoggerFactory.getLogger(DeltalakeMetadataHandlerTest.class);

    protected static int S3_ENDPOINT_PORT = 8001;
    protected static String S3_ENDPOINT = String.format("http://localhost:%d", S3_ENDPOINT_PORT);
    protected static String S3_REGION = "us-east-1";
    protected static String S3_RESOURCES_FOLDER = "/s3";
    protected String spillBucket = "spill-bucket";
    protected String spillPrefix = "spill-prefix";

    private static S3Mock api;
    protected static AmazonS3 amazonS3;

    @BeforeClass
    public static void setupOnce() {
        logger.info("Before all: enter ");
        String bucketPath = DeltalakeMetadataHandlerTest.class.getResource(S3_RESOURCES_FOLDER).getPath();
        api = new S3Mock.Builder().withPort(S3_ENDPOINT_PORT).withFileBackend(bucketPath).build();
        api.start();

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, S3_REGION);
        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

    }

    @AfterClass
    public static void tearDownOnce()
    {
        amazonS3.shutdown();
        api.p().listBuckets().buckets()
                .map(Bucket::name)
                .filter(bucketName -> bucketName.endsWith(".metadata"))
                .foreach(bucketName -> {
                    api.p().deleteBucket(bucketName);
                    return null;
                });
        api.stop();
        api.shutdown();
    }

    protected static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }

    protected SpillLocation makeSpillLocation(String queryId, String splitId)
    {
        return S3SpillLocation.newBuilder()
                .withBucket(spillBucket)
                .withPrefix(spillPrefix)
                .withQueryId(queryId)
                .withSplitId(splitId)
                .build();
    }
}
