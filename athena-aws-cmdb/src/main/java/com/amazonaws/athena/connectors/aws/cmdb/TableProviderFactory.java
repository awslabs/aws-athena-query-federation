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

    public Map<TableName, TableProvider> getTableProviders()
    {
        return tableProviders;
    }

    public Map<String, List<TableName>> getSchemas()
    {
        return schemas;
    }
}
