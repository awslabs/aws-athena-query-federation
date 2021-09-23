/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PublishScriptTest
{
    private static final String publishScriptLocation = "../tools/publish.sh";

    @Test
    public void testBucketPolicy()
            throws IOException
    {
        // The importance of this test is that "aws:SourceAccount" condition doesn't
        // get removed from the S3 bucket policy. This is needed since a service principal
        // will be accessing the bucket on behalf of the calling identity
        String expected =
                "{\n" +
                "  \"Version\": \"2012-10-17\",\n" +
                "  \"Statement\": [\n" +
                "    {\n" +
                "      \"Effect\": \"Allow\",\n" +
                "      \"Principal\": {\n" +
                "        \"Service\":  \"serverlessrepo.amazonaws.com\"\n" +
                "      },\n" +
                "      \"Action\": \"s3:GetObject\",\n" +
                "      \"Resource\": \"arn:$PARTITION:s3:::$1/*\",\n" +
                "      \"Condition\": {\n" +
                "        \"StringEquals\": {\n" +
                "            \"aws:SourceAccount\": \"$account\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";

        StringBuilder actual = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new FileReader(publishScriptLocation))) {
            String line = br.readLine();
            boolean capture = false;

            while (line != null) {
                if (line.contains("EOM")) {
                    capture = !capture;
                }
                else if (capture) {
                    actual.append(line);
                    actual.append("\n");
                }
                line = br.readLine();
            }
        }
        assertEquals(expected, actual.toString());
    }
}
