import * as cdk from 'aws-cdk-lib';
import { aws_redshift as cfnredshift } from 'aws-cdk-lib';
import * as redshift from '@aws-cdk/aws-redshift-alpha';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { CfnInclude } from 'aws-cdk-lib/cloudformation-include';
import { Construct } from 'constructs';
const path = require('path');
import tpcdsJson from '../../resources/tpcds_specs.json'
import {FederationStackProps} from './stack-props'

export class RedshiftStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props?: FederationStackProps) {
    super(scope, id, props);
    const test_size_gigabytes = props!.test_size_gigabytes;
    const s3_path = props!.s3_path;
    const spill_bucket = props!.spill_bucket;
    const tpcds_table_names = props!.tpcds_tables;
    const password = props!.password;
    const connector_yaml_path = props!.connector_yaml_path;

    const vpc = new ec2.Vpc(this, 'redshift_vpc', {
        ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/24'),
        subnetConfiguration: [
          {
            name: 'redshift_public',
            subnetType: ec2.SubnetType.PUBLIC
          },
          {
            name: 'redshift_private',
            subnetType: ec2.SubnetType.PRIVATE_ISOLATED
          }
        ],
        gatewayEndpoints: {
          S3: { service: ec2.GatewayVpcEndpointAwsService.S3 }
        }
    });

    const glueInterfaceVpcEndpoint = new ec2.InterfaceVpcEndpoint(this, 'glue_interface_vpc_endpoint', {
      vpc,
      service: ec2.InterfaceVpcEndpointAwsService.GLUE
    });

    const securityGroup = new ec2.SecurityGroup(this, 'redshift_security_group', {
        vpc: vpc
    });

    securityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(5439));
    securityGroup.addIngressRule(securityGroup, ec2.Port.allTcp());

    // https://github.com/aws/aws-cdk/blob/main/packages/%40aws-cdk/aws-redshift/lib/cluster.ts
    // Original L2 Construct
    const cluster = new redshift.Cluster(this, 'redshift_cluster', {
        numberOfNodes: 2,
        port: 5439,
        vpc: vpc,
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED
        },
        securityGroups: [ securityGroup ],
        masterUser: {
          masterUsername: 'athena',
          masterPassword: cdk.SecretValue.unsafePlainText(password)
        },
        defaultDatabaseName: 'test',
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        publiclyAccessible: false // this is the default but just to be explicit
    });
    cluster.addToParameterGroup('enable_case_sensitive_identifier', 'true');
    
    const s3Spill = new s3.Bucket(this, 'redshift_spill_location', {});

    const connectionString = `jdbc:redshift://${cluster.clusterEndpoint.socketAddress}/test?user=athena&password=${password}`;
    const subnet = vpc.isolatedSubnets[0];
    const glueConnection = new glue.Connection(this, 'redshift_glue_connection', {
      type: glue.ConnectionType.JDBC,
      connectionName: 'redshift_GlueConnectionToVpc',
      securityGroups: [ securityGroup ],
      subnet: vpc.isolatedSubnets[0], // pick any
      properties: {
        JDBC_CONNECTION_URL: connectionString, 
        JDBC_ENFORCE_SSL: 'false',
        USERNAME: 'athena',
        PASSWORD: password, 
        VPC: vpc.vpcId
      }
    });

    const glue_role = new iam.Role(this, 'glue-job-managed-role', {
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName("AdministratorAccess")
        ],
        assumedBy: new iam.ServicePrincipal("glue.amazonaws.com")
    });

    for (var tableName of tpcds_table_names) {
      new glue.Job(this, `redshift_glue_job_${tableName}`, {
        executable: glue.JobExecutable.pythonEtl({
          glueVersion: glue.GlueVersion.V4_0,
          pythonVersion: glue.PythonVersion.THREE,
          script: glue.Code.fromAsset(path.join(__dirname, '../../../glue_scripts/redshift.py'))
        }),
        connections: [ glueConnection ],
        role: glue_role,
        defaultArguments: {
          '--s3_full_prefix': s3_path, 
          '--db_url': `jdbc:redshift://${cluster.clusterEndpoint.socketAddress}/test`,
          '--username': 'athena',
          '--password': password, 
          '--redshiftTmpDir': `s3://${s3Spill.bucketName}/tmpDir`,
          '--tpcds_table_name': tableName
        }
      });
    }

    const glueJob = new glue.Job(this, 'redshift_glue_job_create_case_insensitive_data', {
      executable: glue.JobExecutable.pythonShell({
        glueVersion: glue.GlueVersion.V1_0,
        pythonVersion: glue.PythonVersion.THREE_NINE,
        script: glue.Code.fromAsset(path.join(__dirname, `../../../glue_scripts/redshift_create_case_insensitive_data.py`))
      }),
      role: glue_role,
      connections: [
        glueConnection
      ],
      defaultArguments: {
        '--db_url': cluster.clusterEndpoint.hostname,
        '--username': 'athena',
        '--password': password
      }
    });

    var connectionStringPrefix = 'redshift';
    const cfn_template_file = connector_yaml_path;
    const connectorSubStack = new CfnInclude(this, 'RedshiftLambdaStack', {
      templateFile: cfn_template_file,
      parameters: {
        'LambdaFunctionName': 'redshift-cdk-deployed',
        'SecretNamePrefix': 'asdf',
        'DefaultConnectionString': `${connectionStringPrefix}://${connectionString}`,
        'SecurityGroupIds': [securityGroup.securityGroupId],
        'SubnetIds': [subnet.subnetId],
        'SpillBucket': spill_bucket,
      }
    });
  }

}
