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

/**
 * Constants for Substrait function names used in expression parsing.
 */
public final class SubstraitFunctionNames
{
    private SubstraitFunctionNames()
    {
        // Utility class - prevent instantiation
    }

    // Logical operators
    public static final String NOT_BOOL = "not:bool";
    public static final String AND_BOOL = "and:bool";
    public static final String OR_BOOL = "or:bool";

    // Comparison operators
    public static final String GT_ANY_ANY = "gt:any_any";
    public static final String GT_PTS_PTS = "gt:pts_pts";
    public static final String GTE_ANY_ANY = "gte:any_any";
    public static final String GTE_PTS_PTS = "gte:pts_pts";
    public static final String LT_ANY_ANY = "lt:any_any";
    public static final String LT_PTS_PTS = "lt:pts_pts";
    public static final String LTE_ANY_ANY = "lte:any_any";
    public static final String LTE_PTS_PTS = "lte:pts_pts";
    public static final String EQUAL_ANY_ANY = "equal:any_any";
    public static final String NOT_EQUAL_ANY_ANY = "not_equal:any_any";

    // Null check operators
    public static final String IS_NULL_ANY = "is_null:any";
    public static final String IS_NOT_NULL_ANY = "is_not_null:any";
}
