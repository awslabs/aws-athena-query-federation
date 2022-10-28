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
package com.athena.connectors.msk.trino;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.Plugin;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.optimizations.PlanOptimizer;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface QueryExecutor
{
    void installPlugin(Plugin plugin);

    void createCatalog(String catalogName, String connectorName, Map<String, String> properties);

    TrinoRecordSet execute(String sql);

    TrinoRecordSet execute(Session session, String sql);

    MskTrinoQueryExecutor.PlannedTrinoRecord executeWithPlan(Session session, String sql);

    <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer);

    Plan createPlan(Session session, String sql, LogicalPlanner.Stage stage, boolean forceSingleNode,
                    WarningCollector warningCollector);

    List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode);

    Plan createPlan(Session session, String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage,
                    WarningCollector warningCollector);

    PlannerContext getPlannerContext();

    SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode);
}
