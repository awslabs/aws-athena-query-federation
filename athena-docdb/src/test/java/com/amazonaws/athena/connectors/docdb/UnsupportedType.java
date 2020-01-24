/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb;

/**
 * This is a place holder that is used as a type we won't really be able to coerce meaningfully with
 * a blanket strategy even as we add increasing type support. Basically, we wanted a type that we'd
 * never expect to actually get asked to add support for so that we know when these tests break.
 */
public class UnsupportedType
{

    @Override
    public String toString()
    {
        return "UnsupportedType{}";
    }
}
