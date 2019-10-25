package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.aws.cmdb.tables.EmrClusterTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.RdsTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.EbsTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.Ec2TableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.ImagesTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.RouteTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.SecurityGroupsTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.SubnetTableProvider;
import com.amazonaws.athena.connectors.aws.cmdb.tables.ec2.VpcTableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import org.apache.arrow.util.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Acts as a factory for all supported TableProviders and also a source of meta-data about the
 * schemas and tables that the loaded TableProviders support.
 */
public class TableProviderFactory
{
    private Map<String, List<TableName>> schemas = new HashMap<>();
    private Map<TableName, TableProvider> tableProviders = new HashMap<>();

    public TableProviderFactory()
    {
        this(AmazonEC2ClientBuilder.standard().build(),
                AmazonElasticMapReduceClientBuilder.standard().build(),
                AmazonRDSClientBuilder.standard().build());
    }

    @VisibleForTesting
    protected TableProviderFactory(AmazonEC2 ec2, AmazonElasticMapReduce emr, AmazonRDS rds)
    {
        addProvider(new Ec2TableProvider(ec2));
        addProvider(new EbsTableProvider(ec2));
        addProvider(new VpcTableProvider(ec2));
        addProvider(new SecurityGroupsTableProvider(ec2));
        addProvider(new RouteTableProvider(ec2));
        addProvider(new SubnetTableProvider(ec2));
        addProvider(new ImagesTableProvider(ec2));
        addProvider(new EmrClusterTableProvider(emr));
        addProvider(new RdsTableProvider(rds));
    }

    /**
     * Adds a new TableProvider to the loaded set, if and only if, no existing TableProvider is known
     * for the fully qualified table represented by the new TableProvider we are attempting to add.
     *
     * @param provider The TableProvider to add.
     */
    private void addProvider(TableProvider provider)
    {
        if (tableProviders.putIfAbsent(provider.getTableName(), provider) != null) {
            throw new RuntimeException("Duplicate provider for " + provider.getTableName());
        }

        List<TableName> tables = schemas.get(provider.getSchema());
        if (tables == null) {
            tables = new ArrayList<>();
            schemas.put(provider.getSchema(), tables);
        }
        tables.add(provider.getTableName());
    }

    /**
     * Provides access to the mapping of loaded TableProviders by their fully qualified table names.
     *
     * @return Map of TableNames to their corresponding TableProvider.
     */
    public Map<TableName, TableProvider> getTableProviders()
    {
        return tableProviders;
    }

    /**
     * Provides access to the mapping of TableNames for each schema name discovered during the TableProvider
     * scann.
     *
     * @return Map of schema names to their corresponding list of fully qualified TableNames.
     */
    public Map<String, List<TableName>> getSchemas()
    {
        return schemas;
    }
}
