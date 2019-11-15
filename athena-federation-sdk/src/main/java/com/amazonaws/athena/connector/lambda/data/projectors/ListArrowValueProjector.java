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

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ListArrowValueProjector
        extends ArrowValueProjectorImpl
{
    private final FieldReader listReader;
    private final Projection projection;

    public ListArrowValueProjector(FieldReader listReader)
    {
        this.listReader = requireNonNull(listReader, "listReader is null");

        List<Field> children = listReader.getField().getChildren();
        if (children.size() != 1) {
            throw new RuntimeException("Unexpected number of children for ListProjector field "
                    + listReader.getField().getName());
        }
        Types.MinorType minorType = Types.getMinorTypeForArrowType(children.get(0).getType());
        projection = createValueProjection(minorType);
    }

    @Override
    public Object project(int pos)
    {
        listReader.setPosition(pos);
        if (!listReader.isSet()) {
            return null;
        }

        return doProject();
    }

    protected Object doProject()
    {
        List<Object> list = new ArrayList<>();

        while (listReader.next()) {
            FieldReader subReader = listReader.reader(); // same reader with different idx
            if (!subReader.isSet()) {
                list.add(null);
                continue;
            }

            Object value = projection.doProjection(subReader);
            list.add(value);
        }
        return list;
    }
}
