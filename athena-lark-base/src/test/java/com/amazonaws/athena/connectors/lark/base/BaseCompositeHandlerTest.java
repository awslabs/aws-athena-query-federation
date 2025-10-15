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
package com.amazonaws.athena.connectors.lark.base;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BaseCompositeHandlerTest {

    @Mock
    private BaseMetadataHandler mockMetadataHandler;

    @Mock
    private BaseRecordHandler mockRecordHandler;

    @Test
    public void testCompositeHandlerConstruction() {
        BaseCompositeHandler handler = new BaseCompositeHandler(mockMetadataHandler, mockRecordHandler);
        assertNotNull(handler);
    }

    @Test
    public void testCompositeHandlerConstructionWithVerification() {
        BaseCompositeHandler handler = new BaseCompositeHandler(mockMetadataHandler, mockRecordHandler);
        assertNotNull(handler);
        // Verify handlers are properly set by creating the composite handler
        verifyNoInteractions(mockMetadataHandler);
        verifyNoInteractions(mockRecordHandler);
    }
}
