/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.domain.TableName;

import java.util.Objects;

public class CloudwatchTableName
{
    private final String logGroupName;
    private final String logStreamName;

    public CloudwatchTableName(String logGroupName, String logStreamName)
    {
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
    }

    public String getLogGroupName()
    {
        return logGroupName;
    }

    public String getLogStreamName()
    {
        return logStreamName;
    }

    public TableName toTableName()
    {
        return new TableName(logGroupName.toLowerCase(), logStreamName.toLowerCase());
    }

    @Override
    public String toString()
    {
        return "CloudwatchTableName{" +
                "logGroupName='" + logGroupName + '\'' +
                ", logStreamName='" + logStreamName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CloudwatchTableName that = (CloudwatchTableName) o;
        return Objects.equals(getLogGroupName(), that.getLogGroupName()) &&
                Objects.equals(getLogStreamName(), that.getLogStreamName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getLogGroupName(), getLogStreamName());
    }
}
