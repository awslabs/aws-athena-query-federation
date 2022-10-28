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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProviderModule;
import io.trino.connector.ConnectorManager;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.system.AnalyzePropertiesSystemTable;
import io.trino.connector.system.CatalogSystemTable;
import io.trino.connector.system.ColumnPropertiesSystemTable;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.connector.system.MaterializedViewPropertiesSystemTable;
import io.trino.connector.system.MaterializedViewSystemTable;
import io.trino.connector.system.NodeSystemTable;
import io.trino.connector.system.SchemaPropertiesSystemTable;
import io.trino.connector.system.TableCommentSystemTable;
import io.trino.connector.system.TablePropertiesSystemTable;
import io.trino.connector.system.TransactionsSystemTable;
import io.trino.cost.ComposableStatsCalculator;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculatorUsingExchanges;
import io.trino.cost.CostCalculatorWithEstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.FilterStatsCalculator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsCalculatorModule;
import io.trino.cost.StatsNormalizer;
import io.trino.cost.TaskCountEstimator;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryPreparer;
import io.trino.execution.ScheduledSplit;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.resourcegroups.NoOpResourceGroupManager;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.index.IndexManager;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.ExchangeHandleResolver;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.SchemaPropertyManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.Split;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OutputFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.operator.TaskContext;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.scalar.json.JsonExistsFunction;
import io.trino.operator.scalar.json.JsonQueryFunction;
import io.trino.operator.scalar.json.JsonValueFunction;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.security.GroupProviderManager;
import io.trino.server.PluginManager;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorConfig;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.Plugin;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.GenericPartitioningSpillerFactory;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.spiller.NodeSpillConfig;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SpillerFactory;
import io.trino.spiller.SpillerStats;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizers;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.rewrite.DescribeInputRewrite;
import io.trino.sql.rewrite.DescribeOutputRewrite;
import io.trino.sql.rewrite.ExplainRewrite;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.ShowStatsRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.testing.PageConsumerOperator;
import io.trino.testing.TestingTaskContext;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.type.JsonPath2016Type;
import io.trino.type.TypeDeserializer;
import io.trino.util.FinalizerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.connector.CatalogServiceProviderModule.createAccessControlProvider;
import static io.trino.connector.CatalogServiceProviderModule.createAnalyzePropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createColumnPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createIndexProvider;
import static io.trino.connector.CatalogServiceProviderModule.createMaterializedViewPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createNodePartitioningProvider;
import static io.trino.connector.CatalogServiceProviderModule.createPageSinkProvider;
import static io.trino.connector.CatalogServiceProviderModule.createPageSourceProvider;
import static io.trino.connector.CatalogServiceProviderModule.createSchemaPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createSplitManagerProvider;
import static io.trino.connector.CatalogServiceProviderModule.createTableFunctionProvider;
import static io.trino.connector.CatalogServiceProviderModule.createTableProceduresPropertyManager;
import static io.trino.connector.CatalogServiceProviderModule.createTableProceduresProvider;
import static io.trino.connector.CatalogServiceProviderModule.createTablePropertyManager;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static io.trino.transaction.TransactionBuilder.transaction;
import static io.trino.version.EmbedVersion.testingVersionEmbedder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class MskTrinoQueryExecutor implements QueryExecutor
{
    private final NodeSpillConfig nodeSpillConfig;
    private final boolean alwaysRevokeMemory;
    private final OperatorFactories operatorFactories;

    private final Session defaultSession;
    private final ExecutorService notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;

    private final SqlParser sqlParser;
    private final PlanFragmenter planFragmenter;
    private final InMemoryNodeManager nodeManager;
    private final BlockTypeOperators blockTypeOperators;
    private final PlannerContext plannerContext;
    private final StatsCalculator statsCalculator;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final CostCalculator costCalculator;
    private final CostCalculator estimatedExchangesCostCalculator;
    private final TaskCountEstimator taskCountEstimator;
    // TODO: We need to check production code and customize
    private final MskAccessControlManager accessControl;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final TransactionManager transactionManager;
    private final FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private final SpillerFactory spillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;

    private final PageFunctionCompiler pageFunctionCompiler;
    private final ExpressionCompiler expressionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final JoinCompiler joinCompiler;
    private final ConnectorManager connectorManager;
    private final PluginManager pluginManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    private final TaskManagerConfig taskManagerConfig;
    private final OptimizerConfig optimizerConfig;
    private final StatementAnalyzerFactory statementAnalyzerFactory;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public MskTrinoQueryExecutor(Session session)
    {
        this(session,
                new FeaturesConfig(),
                new NodeSpillConfig(),
                false,
                false,
                0,
                ImmutableMap.of(),
                MetadataManager::new,
                new TrinoOperatorFactories(),
                ImmutableSet.of());
    }

    private MskTrinoQueryExecutor(
            Session defaultSession,
            FeaturesConfig featuresConfig,
            NodeSpillConfig nodeSpillConfig,
            boolean withInitialTransaction,
            boolean alwaysRevokeMemory,
            int nodeCountForStats,
            Map<String, List<PropertyMetadata<?>>> defaultSessionProperties,
            MetadataProvider metadataProvider,
            OperatorFactories operatorFactories,
            Set<SystemSessionPropertiesProvider> extraSessionProperties)
    {
        requireNonNull(defaultSession, "defaultSession is null");
        requireNonNull(defaultSessionProperties, "defaultSessionProperties is null");
        checkArgument(defaultSession.getTransactionId().isEmpty() || !withInitialTransaction, "Already in transaction");

        this.taskManagerConfig = new TaskManagerConfig().setTaskConcurrency(4);
        this.nodeSpillConfig = requireNonNull(nodeSpillConfig, "nodeSpillConfig is null");
        this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
        this.alwaysRevokeMemory = alwaysRevokeMemory;
        this.notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        this.yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        FinalizerService finalizerService = new FinalizerService();
        finalizerService.start();

        TypeOperators typeOperators = new TypeOperators();
        this.blockTypeOperators = new BlockTypeOperators(typeOperators);
        this.sqlParser = new SqlParser();
        this.nodeManager = new InMemoryNodeManager();
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig().setIncludeCoordinator(true);
        requireNonNull(featuresConfig, "featuresConfig is null");
        this.optimizerConfig = new OptimizerConfig();
        CatalogManager catalogManager = new CatalogManager();
        this.transactionManager = InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);

        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        TypeRegistry typeRegistry = new TypeRegistry(typeOperators, featuresConfig);
        TypeManager typeManager = new InternalTypeManager(typeRegistry);
        InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodingManager, typeManager);

        GlobalFunctionCatalog globalFunctionCatalog = new GlobalFunctionCatalog();
        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(blockEncodingSerde)));
        globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(featuresConfig, typeOperators, blockTypeOperators, nodeManager.getCurrentNode().getNodeVersion()));
        FunctionManager functionManager = new FunctionManager(globalFunctionCatalog);
        Metadata metadata = metadataProvider.getMetadata(
                new DisabledSystemSecurityMetadata(),
                transactionManager,
                globalFunctionCatalog,
                typeManager);
        globalFunctionCatalog.addFunctions(new InternalFunctionBundle(
                new JsonExistsFunction(functionManager, metadata, typeManager),
                new JsonValueFunction(functionManager, metadata, typeManager),
                new JsonQueryFunction(functionManager, metadata, typeManager)));
        typeRegistry.addType(new JsonPath2016Type(new TypeDeserializer(typeManager), blockEncodingSerde));
        this.plannerContext = new PlannerContext(metadata, typeOperators, blockEncodingSerde, typeManager, functionManager);
        this.joinCompiler = new JoinCompiler(typeOperators);
        PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(joinCompiler, blockTypeOperators);
        // TODO: To be a no-op provider, no need to protect a trino server, there are no such
        // We need to customize it if it's DB specific
        GroupProvider groupProvider = new NoopGroupProvider();
        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig());
        this.accessControl = new MskAccessControlManager(transactionManager, eventListenerManager);
        accessControl.loadSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());

        this.pageFunctionCompiler = new PageFunctionCompiler(functionManager, 0);
        this.expressionCompiler = new ExpressionCompiler(functionManager, pageFunctionCompiler);
        this.joinFilterFunctionCompiler = new JoinFilterFunctionCompiler(functionManager);

        HandleResolver handleResolver = new HandleResolver();

        NodeInfo nodeInfo = new NodeInfo("test");
        CatalogFactory catalogFactory = new DefaultCatalogFactory(
                metadata,
                accessControl,
                handleResolver,
                nodeManager,
                pageSorter,
                pageIndexerFactory,
                nodeInfo,
                testingVersionEmbedder(),
                transactionManager,
                typeManager,
                nodeSchedulerConfig);
        this.connectorManager = new ConnectorManager(catalogFactory, catalogManager);
        this.splitManager = new SplitManager(createSplitManagerProvider(connectorManager), new QueryManagerConfig());
        this.pageSourceManager = new PageSourceManager(createPageSourceProvider(connectorManager));
        this.pageSinkManager = new PageSinkManager(createPageSinkProvider(connectorManager));
        this.indexManager = new IndexManager(createIndexProvider(connectorManager));
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, new NodeTaskMap(finalizerService)));
        this.sessionPropertyManager = createSessionPropertyManager(connectorManager, extraSessionProperties, taskManagerConfig, featuresConfig, optimizerConfig);
        this.nodePartitioningManager = new NodePartitioningManager(nodeScheduler, blockTypeOperators, createNodePartitioningProvider(connectorManager));
        TableProceduresRegistry tableProceduresRegistry = new TableProceduresRegistry(createTableProceduresProvider(connectorManager));
        TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(createTableFunctionProvider(connectorManager));
        this.schemaPropertyManager = createSchemaPropertyManager(connectorManager);
        this.columnPropertyManager = createColumnPropertyManager(connectorManager);
        this.tablePropertyManager = createTablePropertyManager(connectorManager);
        this.materializedViewPropertyManager = createMaterializedViewPropertyManager(connectorManager);
        AnalyzePropertyManager analyzePropertyManager = createAnalyzePropertyManager(connectorManager);
        TableProceduresPropertyManager tableProceduresPropertyManager = createTableProceduresPropertyManager(connectorManager);

        accessControl.setConnectorAccessControlProvider(createAccessControlProvider(connectorManager));

        this.statementAnalyzerFactory = new StatementAnalyzerFactory(
                plannerContext,
                sqlParser,
                accessControl,
                transactionManager,
                groupProvider,
                tableProceduresRegistry,
                tableFunctionRegistry,
                sessionPropertyManager,
                tablePropertyManager,
                analyzePropertyManager,
                tableProceduresPropertyManager);
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(plannerContext, statementAnalyzerFactory);
        this.statsCalculator = createNewStatsCalculator(plannerContext, typeAnalyzer);
        this.scalarStatsCalculator = new ScalarStatsCalculator(plannerContext, typeAnalyzer);
        this.taskCountEstimator = new TaskCountEstimator(() -> nodeCountForStats);
        this.costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        this.estimatedExchangesCostCalculator = new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);

        this.planFragmenter = new PlanFragmenter(metadata, functionManager, new QueryManagerConfig());

        GlobalSystemConnector globalSystemConnector = new GlobalSystemConnector(ImmutableSet.of(
                new NodeSystemTable(nodeManager),
                new CatalogSystemTable(metadata, accessControl),
                new TableCommentSystemTable(metadata, accessControl),
                new MaterializedViewSystemTable(metadata, accessControl),
                new SchemaPropertiesSystemTable(transactionManager, schemaPropertyManager),
                new TablePropertiesSystemTable(transactionManager, tablePropertyManager),
                new MaterializedViewPropertiesSystemTable(transactionManager, materializedViewPropertyManager),
                new ColumnPropertiesSystemTable(transactionManager, columnPropertyManager),
                new AnalyzePropertiesSystemTable(transactionManager, analyzePropertyManager),
                new TransactionsSystemTable(typeManager, transactionManager)),
                ImmutableSet.of());

        exchangeManagerRegistry = new ExchangeManagerRegistry(new ExchangeHandleResolver());
        this.pluginManager = new PluginManager(
                (loader, createClassLoader) -> {
                },
                catalogFactory,
                globalFunctionCatalog,
                new NoOpResourceGroupManager(),
                accessControl,
                Optional.of(new PasswordAuthenticatorManager(new PasswordAuthenticatorConfig())),
                new CertificateAuthenticatorManager(),
                Optional.of(new HeaderAuthenticatorManager(new HeaderAuthenticatorConfig())),
                eventListenerManager,
                new GroupProviderManager(),
                new SessionPropertyDefaults(nodeInfo, accessControl),
                typeRegistry,
                blockEncodingManager,
                handleResolver,
                exchangeManagerRegistry);

        connectorManager.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, GlobalSystemConnector.NAME, globalSystemConnector);

        // rewrite session to use managed SessionPropertyMetadata
        Optional<TransactionId> transactionId = withInitialTransaction ? Optional.of(transactionManager.beginTransaction(true)) : defaultSession.getTransactionId();
        this.defaultSession = new Session(
                defaultSession.getQueryId(),
                transactionId,
                defaultSession.isClientTransactionSupport(),
                defaultSession.getIdentity(),
                defaultSession.getSource(),
                defaultSession.getCatalog(),
                defaultSession.getSchema(),
                defaultSession.getPath(),
                defaultSession.getTraceToken(),
                defaultSession.getTimeZoneKey(),
                defaultSession.getLocale(),
                defaultSession.getRemoteUserAddress(),
                defaultSession.getUserAgent(),
                defaultSession.getClientInfo(),
                defaultSession.getClientTags(),
                defaultSession.getClientCapabilities(),
                defaultSession.getResourceEstimates(),
                defaultSession.getStart(),
                defaultSession.getSystemProperties(),
                defaultSession.getCatalogProperties(),
                sessionPropertyManager,
                defaultSession.getPreparedStatements(),
                defaultSession.getProtocolHeaders());

        SpillerStats spillerStats = new SpillerStats();
        this.singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(plannerContext.getBlockEncodingSerde(), spillerStats, featuresConfig, nodeSpillConfig);
        this.partitioningSpillerFactory = new GenericPartitioningSpillerFactory(this.singleStreamSpillerFactory);
        this.spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin, ignored -> plugin.getClass().getClassLoader());
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        CatalogHandle catalogHandle = connectorManager.createCatalog(catalogName, connectorName, properties);
        nodeManager.addCurrentNodeCatalog(catalogHandle);
    }

    public interface MetadataProvider
    {
        Metadata getMetadata(
                SystemSecurityMetadata systemSecurityMetadata,
                TransactionManager transactionManager,
                GlobalFunctionCatalog globalFunctionCatalog,
                TypeManager typeManager);
    }

    @Override
    public TrinoRecordSet execute(String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public TrinoRecordSet execute(Session session, String sql)
    {
        return executeWithPlan(session, sql).getTrinoRecords();
    }

    @Override
    public PlannedTrinoRecord executeWithPlan(Session session, String sql)
    {
        return inTransaction(session, transactionSession -> executeInternal(transactionSession, sql));
    }

    @Override
    public <T> T inTransaction(Session session, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, transactionSessionConsumer);
    }

    @Override
    public Plan createPlan(Session session, String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, WarningCollector warningCollector)
    {
        QueryPreparer.PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);
        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());
        return createPlan(session, sql, getPlanOptimizers(forceSingleNode), stage, warningCollector);
    }

    // TODO: Check production code
    @Override
    public List<PlanOptimizer> getPlanOptimizers(boolean forceSingleNode)
    {
        return new PlanOptimizers(
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                taskManagerConfig,
                forceSingleNode,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(optimizerConfig),
                taskCountEstimator,
                nodePartitioningManager,
                new RuleStatsRecorder()).get();
    }

    // TODO: Check production code
    @Override
    public Plan createPlan(Session session, String sql, List<PlanOptimizer> optimizers, LogicalPlanner.Stage stage, WarningCollector warningCollector)
    {
        QueryPreparer.PreparedQuery preparedQuery = new QueryPreparer(sqlParser).prepareQuery(session, sql);

        assertFormattedSql(sqlParser, createParsingOptions(session), preparedQuery.getStatement());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        AnalyzerFactory analyzerFactory = createAnalyzerFactory(createQueryExplainerFactory(optimizers));
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                session,
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                optimizers,
                new PlanSanityChecker(true),
                idAllocator,
                getPlannerContext(),
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                statsCalculator,
                costCalculator,
                warningCollector);

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());
        // make QueryExecutor always compute plan statistics for test purposes
        return logicalPlanner.plan(analysis, stage);
    }

    @Override
    public PlannerContext getPlannerContext()
    {
        return plannerContext;
    }

    @Override
    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode)
    {
        return planFragmenter.createSubPlans(session, plan, forceSingleNode, WarningCollector.NOOP);
    }

    // helpers
    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(1000)).getSplits();
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    private QueryExplainerFactory createQueryExplainerFactory(List<PlanOptimizer> optimizers)
    {
        return new QueryExplainerFactory(
                () -> optimizers,
                planFragmenter,
                plannerContext,
                statementAnalyzerFactory,
                statsCalculator,
                costCalculator);
    }

    private static StatsCalculator createNewStatsCalculator(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        StatsNormalizer normalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(plannerContext, typeAnalyzer);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(plannerContext, scalarStatsCalculator, normalizer);
        return new ComposableStatsCalculator(new StatsCalculatorModule.StatsRulesProvider(plannerContext, scalarStatsCalculator, filterStatsCalculator, normalizer).get());
    }

    private AnalyzerFactory createAnalyzerFactory(QueryExplainerFactory queryExplainerFactory)
    {
        return new AnalyzerFactory(
                statementAnalyzerFactory,
                new StatementRewrite(ImmutableSet.of(
                        new DescribeInputRewrite(sqlParser),
                        new DescribeOutputRewrite(sqlParser),
                        new ShowQueriesRewrite(
                                plannerContext.getMetadata(),
                                sqlParser,
                                accessControl,
                                sessionPropertyManager,
                                schemaPropertyManager,
                                columnPropertyManager,
                                tablePropertyManager,
                                materializedViewPropertyManager),
                        new ShowStatsRewrite(plannerContext.getMetadata(), queryExplainerFactory, statsCalculator),
                        new ExplainRewrite(queryExplainerFactory, new QueryPreparer(sqlParser)))));
    }

    private static SessionPropertyManager createSessionPropertyManager(
            ConnectorServicesProvider connectorServicesProvider,
            Set<SystemSessionPropertiesProvider> extraSessionProperties,
            TaskManagerConfig taskManagerConfig,
            FeaturesConfig featuresConfig,
            OptimizerConfig optimizerConfig)
    {
        Set<SystemSessionPropertiesProvider> systemSessionProperties = ImmutableSet.<SystemSessionPropertiesProvider>builder()
                .addAll(requireNonNull(extraSessionProperties, "extraSessionProperties is null"))
                .add(new SystemSessionProperties(
                        new QueryManagerConfig(),
                        taskManagerConfig,
                        new MemoryManagerConfig(),
                        featuresConfig,
                        optimizerConfig,
                        new NodeMemoryConfig(),
                        new DynamicFilterConfig(),
                        new NodeSchedulerConfig()))
                .build();

        return CatalogServiceProviderModule.createSessionPropertyManager(systemSessionProperties, connectorServicesProvider);
    }

    // TODO: Check production code
    private PlannedTrinoRecord executeInternal(Session session, String sql)
    {
        lock.readLock().lock();
        try (Closer closer = Closer.create()) {
            accessControl.checkCanExecuteQuery(session.getIdentity());
            AtomicReference<TrinoRecordSet.Builder> builder = new AtomicReference<>();
            PageConsumerOperator.PageConsumerOutputFactory outputFactory = new PageConsumerOperator.PageConsumerOutputFactory(types -> {
                builder.compareAndSet(null, TrinoRecordSet.resultBuilder(session.toConnectorSession(), types));
                return builder.get()::page;
            });

            TaskContext taskContext = TestingTaskContext.builder(notificationExecutor, yieldExecutor, session)
                    .setMaxSpillSize(nodeSpillConfig.getMaxSpillPerNode())
                    .setQueryMaxSpillSize(nodeSpillConfig.getQueryMaxSpillPerNode())
                    .build();

            Plan plan = createPlan(session, sql, OPTIMIZED_AND_VALIDATED, true, WarningCollector.NOOP);
            List<Driver> drivers = createDrivers(session, plan, outputFactory, taskContext);
            drivers.forEach(closer::register);

            boolean done = false;
            while (!done) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (alwaysRevokeMemory) {
                        driver.getDriverContext().getOperatorContexts().stream()
                                .filter(operatorContext -> operatorContext.getNestedOperatorStats().stream()
                                        .mapToLong(stats -> stats.getRevocableMemoryReservation().toBytes())
                                        .sum() > 0)
                                .forEach(OperatorContext::requestMemoryRevoking);
                    }

                    if (!driver.isFinished()) {
                        driver.processForNumberOfIterations(1);
                        processed = true;
                    }
                }
                done = !processed;
            }

            verify(builder.get() != null, "Output operator was not created");
            return new PlannedTrinoRecord(builder.get().build());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    // TODO: Check production code for non-JDBC
    private List<Driver> createDrivers(Session session, Plan plan, OutputFactory outputFactory, TaskContext taskContext)
    {
        SubPlan subplan = createSubPlans(session, plan, true);
        if (!subplan.getChildren().isEmpty()) {
            throw new AssertionError("Expected sub-plan to have no children");
        }

        TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();
        tableExecuteContextManager.registerTableExecuteContextForQuery(taskContext.getQueryContext().getQueryId());
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                Optional.empty(),
                pageSourceManager,
                indexManager,
                nodePartitioningManager,
                pageSinkManager,
                null,
                expressionCompiler,
                pageFunctionCompiler,
                joinFilterFunctionCompiler,
                new IndexJoinLookupStats(),
                this.taskManagerConfig,
                spillerFactory,
                singleStreamSpillerFactory,
                partitioningSpillerFactory,
                new PagesIndex.TestingFactory(false),
                joinCompiler,
                operatorFactories,
                new OrderingCompiler(plannerContext.getTypeOperators()),
                new DynamicFilterConfig(),
                blockTypeOperators,
                tableExecuteContextManager,
                exchangeManagerRegistry);

        // plan query
        LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = executionPlanner.plan(
                taskContext,
                subplan.getFragment().getRoot(),
                subplan.getFragment().getPartitioningScheme().getOutputLayout(),
                plan.getTypes(),
                subplan.getFragment().getPartitionedSources(),
                outputFactory);

        // generate splitAssignments
        List<SplitAssignment> splitAssignments = new ArrayList<>();
        long sequenceId = 0;
        for (TableScanNode tableScan : findTableScanNodes(subplan.getFragment().getRoot())) {
            TableHandle table = tableScan.getTable();

            SplitSource splitSource = splitManager.getSplits(
                    session,
                    table,
                    EMPTY,
                    alwaysTrue());

            ImmutableSet.Builder<ScheduledSplit> scheduledSplits = ImmutableSet.builder();
            while (!splitSource.isFinished()) {
                for (Split split : getNextBatch(splitSource)) {
                    scheduledSplits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
                }
            }

            splitAssignments.add(new SplitAssignment(tableScan.getId(), scheduledSplits.build(), true));
        }

        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, DriverFactory> driverFactoriesBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                if (driverFactory.getSourceId().isPresent()) {
                    checkState(driverFactoriesBySource.put(driverFactory.getSourceId().get(), driverFactory) == null);
                }
                else {
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false).addDriverContext();
                    Driver driver = driverFactory.createDriver(driverContext);
                    drivers.add(driver);
                }
            }
        }

        // add split assignments to the drivers
        ImmutableSet<PlanNodeId> partitionedSources = ImmutableSet.copyOf(subplan.getFragment().getPartitionedSources());
        for (SplitAssignment splitAssignment : splitAssignments) {
            DriverFactory driverFactory = driverFactoriesBySource.get(splitAssignment.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = partitionedSources.contains(driverFactory.getSourceId().get());
            for (ScheduledSplit split : splitAssignment.getSplits()) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSplitAssignment(new SplitAssignment(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    static class PlannedTrinoRecord
    {
        private final TrinoRecordSet trinoRecords;

        public PlannedTrinoRecord(TrinoRecordSet trinoRecords)
        {
            this.trinoRecords = trinoRecords;
        }

        public TrinoRecordSet getTrinoRecords()
        {
            return trinoRecords;
        }
    }
}
