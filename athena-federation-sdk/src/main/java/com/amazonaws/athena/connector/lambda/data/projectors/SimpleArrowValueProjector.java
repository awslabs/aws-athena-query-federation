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

import static java.util.Objects.requireNonNull;

public class SimpleArrowValueProjector
        extends ArrowValueProjectorImpl
{
    private final Projection projection;
    private final FieldReader fieldReader;

    public SimpleArrowValueProjector(FieldReader fieldReader)
    {
        this.fieldReader = requireNonNull(fieldReader, "fieldReader is null");
        this.projection = createValueProjection(fieldReader.getMinorType());
    }

    @Override
    public Object project(int pos)
    {
        fieldReader.setPosition(pos);
        return projection.doProjection(fieldReader);
    }
}
