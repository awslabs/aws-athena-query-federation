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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.athena.connectors.msk.trino;

import com.google.inject.Inject;
import io.trino.eventlistener.EventListenerManager;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.transaction.TransactionManager;

public class MskAccessControlManager
        extends AccessControlManager
{
    @Inject
    public MskAccessControlManager(TransactionManager transactionManager, EventListenerManager eventListenerManager, AccessControlConfig accessControlConfig)
    {
        super(transactionManager, eventListenerManager, accessControlConfig, DefaultSystemAccessControl.NAME);
    }

    public MskAccessControlManager(TransactionManager transactionManager, EventListenerManager eventListenerManager)
    {
        this(transactionManager, eventListenerManager, new AccessControlConfig());
    }

    // Default implementations of the base work well with MSK so no specific implementation
}
