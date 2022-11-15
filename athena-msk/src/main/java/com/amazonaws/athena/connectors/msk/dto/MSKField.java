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

public class MSKField
{
    String name = "";
    String mapping = "";
    String type = "";
    String formatHint = "";
    Object value = "";

    public MSKField()
    {
    }

    public MSKField(String name, String mapping, String type, String formatHint, Object value)
    {
        this.name = name;
        this.mapping = mapping;
        this.type = type;
        this.formatHint = formatHint;
        this.value = value;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getMapping()
    {
        return mapping;
    }

    public void setMapping(String mapping)
    {
        this.mapping = mapping;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getFormatHint()
    {
        return formatHint;
    }

    public void setFormatHint(String formatHint)
    {
        this.formatHint = formatHint;
    }

    public Object getValue()
    {
        return value;
    }

    public void setValue(Object value)
    {
        this.value = value;
    }

    @Override
    public String toString()
    {
        return "Field{" +
                "name='" + name + '\'' +
                ", mapping='" + mapping + '\'' +
                ", type='" + type + '\'' +
                ", formatHint='" + formatHint + '\'' +
                ", value=" + value +
                '}';
    }
}
