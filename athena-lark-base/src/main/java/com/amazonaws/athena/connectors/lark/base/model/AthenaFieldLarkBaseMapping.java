/*-
 * #%L
 * athena-lark-base
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
package com.amazonaws.athena.connectors.lark.base.model;

/**
 * Record for Athena Field Lark Base Mapping
 *
 * @param athenaName        The Athena name
 * @param larkBaseFieldName The Lark Base Field name
 * @param nestedUIType      The nested UI type that contains the UI type and child type (to handle FORMULA and LOOKUP)
 */
public record AthenaFieldLarkBaseMapping(String athenaName, String larkBaseFieldName, NestedUIType nestedUIType)
{
}
