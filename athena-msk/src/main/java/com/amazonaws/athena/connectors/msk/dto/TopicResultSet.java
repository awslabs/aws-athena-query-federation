/*-
 * #%L
 * Athena MSK Connector
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
package com.amazonaws.athena.connectors.msk.dto;

import java.util.ArrayList;
import java.util.List;

public class TopicResultSet
{
    String topicName;
    String dataFormat;
    List<MSKField> fields = new ArrayList<>();

    public String getTopicName()
    {
        return topicName;
    }

    public void setTopicName(String topicName)
    {
        this.topicName = topicName;
    }

    public String getDataFormat()
    {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat)
    {
        this.dataFormat = dataFormat;
    }

    public List<MSKField> getFields()
    {
        return fields;
    }

    public void setFields(List<MSKField> fields)
    {
        this.fields = fields;
    }

    @Override
    public String toString()
    {
        return "TopicResultSet{" +
                "topicName='" + topicName + '\'' +
                ", dataFormat='" + dataFormat + '\'' +
                ", fields=" + fields +
                '}';
    }
}
