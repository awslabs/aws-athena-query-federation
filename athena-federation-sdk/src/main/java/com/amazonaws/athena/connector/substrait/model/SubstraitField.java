/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.substrait.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a field in a Substrait schema with its name, type, and optional nested children.
 * This class models the hierarchical structure of Substrait fields, supporting complex types
 * like structs that can contain nested fields.
 */
public final class SubstraitField
{
    private final String name;
    private final String type;
    private final List<SubstraitField> children;

    /**
     * Constructs a SubstraitField with the specified name, type, and children.
     * 
     * @param name The name of the field, must not be null
     * @param type The type of the field, must not be null
     * @param children The list of child fields for complex types, may be null or empty
     * @throws IllegalArgumentException if name or type is null
     */
    public SubstraitField(final String name,
                          final String type,
                          final List<SubstraitField> children)
    {
        this.name = requireNonNull(name, "Field name cannot be null");
        this.type = requireNonNull(type, "Field type cannot be null");
        this.children = children != null ? Collections.unmodifiableList(children) : Collections.emptyList();
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public List<SubstraitField> getChildren()
    {
        return children;
    }
    

    public boolean hasChildren()
    {
        return !children.isEmpty();
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
        SubstraitField that = (SubstraitField) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(type, that.type) &&
               Objects.equals(children, that.children);
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, children);
    }
    
    @Override
    public String toString()
    {
        return String.format("SubstraitField{name='%s', type='%s', children=%d}", 
                           name, type, children.size());
    }
}
