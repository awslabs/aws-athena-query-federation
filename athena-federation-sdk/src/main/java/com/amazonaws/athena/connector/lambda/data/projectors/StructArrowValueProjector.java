package com.amazonaws.athena.connector.lambda.data.projectors;

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

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StructArrowValueProjector
        extends ArrowValueProjectorImpl
{
    private final Map<String, Projection> projectionsMap;
    private final FieldReader structReader;

    public StructArrowValueProjector(FieldReader structReader)
    {
        this.structReader = requireNonNull(structReader, "structReader is null");

        ImmutableMap.Builder<String, Projection> projectionMapBuilder = ImmutableMap.builder();

        List<Field> children = structReader.getField().getChildren();

        for (Field child : children) {
            String childName = child.getName();
            Types.MinorType minorType = Types.getMinorTypeForArrowType(child.getType());
            Projection projection = createValueProjection(minorType);
            projectionMapBuilder.put(childName, projection);
        }

        this.projectionsMap = projectionMapBuilder.build();
    }

    @Override
    public Object project(int pos)
    {
        structReader.setPosition(pos);
        if (!structReader.isSet()) {
            return null;
        }

        return doProject();
    }

    protected Map<String, Object> doProject()
    {
        List<Field> fields = structReader.getField().getChildren();
        Map<String, Object> nameToValues = new HashMap<>();
        for (Field child : fields) {
            String childName = child.getName();
            FieldReader subReader = structReader.reader(childName);
            Projection childProjection = projectionsMap.get(childName);
            nameToValues.put(child.getName(), childProjection.doProjection(subReader));
        }
        return nameToValues;
    }
}
