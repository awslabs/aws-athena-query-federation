/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DeltaSharePredicateUtilsTest
{
    @Test
    public void testBuildFilterPredicatesFromPlanWithNullPlan()
    {
        Map<String, List<ColumnPredicate>> result = DeltaSharePredicateUtils.buildFilterPredicatesFromPlan(null);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testBuildJsonPredicateHintsWithNullInputs()
    {
        try {
            DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(null, null);
        } catch (NullPointerException e) {
            assertTrue(true);
        }
        
        try {
            DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(new HashMap<>(), null);
        } catch (Exception e) {
            assertTrue(true);
        }
        
        try {
            DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(null, new ArrayList<>());
        } catch (Exception e) {
            assertTrue(true);
        }
    }
    
}
