import * as cdk from 'aws-cdk-lib';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as iam from 'aws-cdk-lib/aws-iam'
import { CfnInclude } from 'aws-cdk-lib/cloudformation-include';
import { Construct } from 'constructs';
const path = require('path')
import { FederationStackProps } from './stack-props'

export interface RdsGenericStackProps extends FederationStackProps {
  readonly db_port: number;
  readonly db_type: string;
}

export class RdsGenericStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: RdsGenericStackProps) {
    super(scope, id, props);
    this.init_resources(props!.test_size_gigabytes, props!.s3_path, props!.password, props!.tpcds_tables, props!.db_port, props!.db_type, props!.connector_yaml_path);
  }

  init_resources(test_size_gigabytes: number, s3_path: string, password: string, tpcds_tables: string[], db_port: number, db_type: string, connector_yaml_path: string) {
    const vpc = new ec2.Vpc(this, `${db_type}_vpc`, {
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/24'),
      subnetConfiguration: [
        {
          name: `${db_type}_public`,
          subnetType: ec2.SubnetType.PUBLIC
        },
        {
          name: `${db_type}_private`,
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED
        }
      ],
      gatewayEndpoints: {
        S3: {
          service: ec2.GatewayVpcEndpointAwsService.S3,
        },
      },
    });

    // add glue interface endpiont (different from gateway endpoint)
    const glueInterfaceVpcEndpoint = new ec2.InterfaceVpcEndpoint(this, 'glue_interface_vpc_endpoint', {
      vpc,
      service: ec2.InterfaceVpcEndpointAwsService.GLUE
    });

    const securityGroup = new ec2.SecurityGroup(this, `${db_type}_security_group`, {
        vpc: vpc
    });

    // open db port
    securityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(db_port));

    // allow comms from the same SG (this lets us write from Glue)
    securityGroup.addIngressRule(securityGroup, ec2.Port.allTcp())

    const cluster = new rds.DatabaseCluster(this, `${db_type}db_cluster`, {
      engine: this.getEngineVersion(db_type),
      port: db_port,
      defaultDatabaseName: "test",
      credentials: {
        username: 'athena',
        password: cdk.SecretValue.unsafePlainText(password)
      },
      instances: 2,
      instanceProps: {
        publiclyAccessible: false,
        vpc: vpc,
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED
        },
        securityGroups: [securityGroup]
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
    const connectionString = `jdbc:${db_type}://${cluster.clusterEndpoint.socketAddress}/test?user=athena&password=${password}`;
    const subnet = vpc.isolatedSubnets[0]; // just pick one

    // make glue connection, use same security group and subnet as cluster above.
    const glueConnection = new glue.Connection(this, `${db_type}_glue_connection`, {
        type: glue.ConnectionType.JDBC,
        connectionName: `${db_type}_GlueConnectionToVpc`,
        securityGroups: [securityGroup],
        subnet: subnet,
        properties: {
          JDBC_CONNECTION_URL: connectionString,
          JDBC_ENFORCE_SSL: 'false',
          USERNAME: 'athena',
          PASSWORD: password,
          VPC: vpc.vpcId,
        }
    });
    var glue_role = new iam.Role(this, 'glue-job-managed-role', {
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("AdministratorAccess")
      ],
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com")
    });

    // now make the glue jobs
    for (var tpcds_table of tpcds_tables) {
      const glueJob = new glue.Job(this, `${db_type}_glue_job_${tpcds_table}`, {
        executable: glue.JobExecutable.pythonEtl({
          glueVersion: glue.GlueVersion.V4_0,
          pythonVersion: glue.PythonVersion.THREE,
          script: glue.Code.fromAsset(path.join(__dirname, `../../../glue_scripts/${db_type}.py`))
        }),
        role: glue_role, 
        connections: [
          glueConnection
        ],
        defaultArguments: {
          '--s3_full_prefix': s3_path, 
          '--db_url': connectionString,
          '--username': "athena",
          '--password': password,
          '--tpcds_table': tpcds_table
        }
      });
    }
    const cfn_template_file = connector_yaml_path;
    var connectionStringPrefix = '';
    if (db_type == 'mysql') connectionStringPrefix = 'mysql';
    if (db_type == 'postgresql') connectionStringPrefix = 'postgres';
    const connectorSubStack = new CfnInclude(this,`${db_type}LambdaStack`, {
      templateFile: cfn_template_file,
      parameters: {
        'LambdaFunctionName': `${db_type}-cdk-deployed`,
        'SecretNamePrefix': 'asdf',
        'DefaultConnectionString': `${connectionStringPrefix}://${connectionString}`,
        'SecurityGroupIds': [securityGroup.securityGroupId],
        'SubnetIds': [subnet.subnetId],
        'SpillBucket': 'amazon-athena-federation-perf-spill-bucket',
      }
    });
  }

  getEngineVersion(db_type: string): rds.IClusterEngine {
    // for some reason, I can't just pass the below in as a property. So we have to just keep
    // a switch here until that's resolved.
    if (db_type == 'mysql') {
      return rds.DatabaseClusterEngine.auroraMysql({version:rds.AuroraMysqlEngineVersion.VER_3_01_0});
    } else if (db_type == 'postgresql') {
      return rds.DatabaseClusterEngine.auroraPostgres({version:rds.AuroraPostgresEngineVersion.VER_13_4});
    } else {
      throw new Error("unsupported rds engine version");
    }
  }
}
