import { Construct } from 'constructs';
import {aws_opensearchservice as opensearch} from 'aws-cdk-lib';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { CfnInclude } from 'aws-cdk-lib/cloudformation-include';
const path = require('path');
import {FederationStackProps} from './stack-props'

function getVolumeSize(test_size_gigabytes: number): number {
  switch(test_size_gigabytes) {
    case 10:
      return 10;
    case 100:
      return 25;
    case 1000:
      return 75;
    default:
      return 150;
  }
}

function getInstanceType(test_size_gigabytes: number): string {
  switch(test_size_gigabytes) {
    case 10:
      return 'r6g.large.search';
    case 100:
      return 'r6g.xlarge.search';
    case 1000:
      return 'r6g.2xlarge.search';
    default:
      return 'r6g.large.search';
  }
}

export class OpenSearchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: FederationStackProps) {
    const volumeSizePerNode = getVolumeSize(props!.test_size_gigabytes);
    const dataInstanceType = getInstanceType(props!.test_size_gigabytes);
    super(scope, id, props);
    this.init_resources(volumeSizePerNode, dataInstanceType, props!.test_size_gigabytes, props!.s3_path, props!.tpcds_tables, props!.password, props!.connector_yaml_path);
  }

  init_resources(volumeSizePerNode: number, dataNodeInstanceType: string, test_size_gigabytes: number, s3_path: string, tpcds_tables: string[], password: string, connector_yaml_path: string){
    const vpc = new ec2.Vpc(this, 'opensearch_vpc', {
      ipAddresses: ec2.IpAddresses.cidr('15.0.0.0/24'),
      subnetConfiguration: [
        {
          name: 'opensearch_public',
          subnetType: ec2.SubnetType.PUBLIC
        },
        {
          name: 'opensearch_private',
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

    // To download Elasticsearch connector for glue job from ECR
    const ecrDkrInterfaceVpcEndpoint = new ec2.InterfaceVpcEndpoint(this, 'ecr_dkr_interface_vpc_endpoint', {
      vpc,
      service: ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER
    });

    const ecrApiInterfaceVpcEndpoint = new ec2.InterfaceVpcEndpoint(this, 'ecr_api_interface_vpc_endpoint', {
      vpc,
      service: ec2.InterfaceVpcEndpointAwsService.ECR
    });

    const securityGroup = new ec2.SecurityGroup(this, 'opensearch_security_group', {
      vpc: vpc
    });
    const subnet = vpc.isolatedSubnets[0]; // just pick one

    // port for Opensearch Domain
    securityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443));
    // glue job communication
    securityGroup.addIngressRule(securityGroup, ec2.Port.allTcp());

    const domain = new opensearch.Domain(this, 'opensearch_domain', {
      version: opensearch.EngineVersion.OPENSEARCH_2_3,
      enableVersionUpgrade: true,
      capacity: {
        masterNodes: 3,
        dataNodes: 10,
        dataNodeInstanceType: dataNodeInstanceType,
        masterNodeInstanceType: 'r6g.xlarge.search'
      },
      ebs: {
        volumeSize: volumeSizePerNode,
        volumeType: ec2.EbsDeviceVolumeType.GENERAL_PURPOSE_SSD,
      },
      zoneAwareness: {
        availabilityZoneCount: 2,
      },
      nodeToNodeEncryption: true,
      encryptionAtRest: {
        enabled: true,
      },
      enforceHttps: true,
      fineGrainedAccessControl: {
        masterUserName: "athena",
        masterUserPassword: cdk.SecretValue.unsafePlainText(password)
      },
      vpc: vpc,
      vpcSubnets: [{
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }],
      securityGroups: [securityGroup],
      advancedOptions: {"override_main_response_version": "true"}
    });
    const connectionString = `https://${domain.domainEndpoint}`;
    const s3Spill = new s3.Bucket(this, 'opensearch_spill_location', {});

    const glueConnection = new glue.Connection(this, 'opensearch_glue_connection', {
      type: new glue.ConnectionType('MARKETPLACE'),
      connectionName: 'opensearch_GlueConnectionToVpc',
      matchCriteria: [
        "Connection",
        "Elasticsearch Connector 7.13.4 for AWS Glue 3.0"
      ],
      securityGroups: [ securityGroup ],
      subnet: subnet, 
      properties: {
        CONNECTOR_TYPE: 'Spark', 
        CONNECTOR_URL: 'https://709825985650.dkr.ecr.us-east-1.amazonaws.com/amazon-web-services/glue/elasticsearch:7.13.4-glue3.0-2', 
        CONNECTOR_CLASS_NAME: 'org.elasticsearch.spark.sql',
        VPC: vpc.vpcId
      }
    });

    new glue.Job(this, 'opensearch_glue_job', {
      executable: glue.JobExecutable.pythonEtl({
        glueVersion: glue.GlueVersion.V4_0,
        pythonVersion: glue.PythonVersion.THREE,
        script: glue.Code.fromAsset(path.join(__dirname, `../../../glue_scripts/opensearch.py`)) 
      }),
      connections: [glueConnection],
      role: new iam.Role(this, 'glue-job-managed-role', {
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName("AdministratorAccess"),
          // Policy that grants permission to download connector from Marketplace
          iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonEC2ContainerRegistryFullAccess")
        ],
        assumedBy: new iam.ServicePrincipal("glue.amazonaws.com")
      }),
      defaultArguments: {
        '--s3_full_prefix': s3_path,
        '--domain_endpoint': connectionString, 
        '--username': 'athena',
        '--password': password,
        '--glue_connection': glueConnection.connectionName
      }
    });
    const cfn_template_file = connector_yaml_path;
    const connectorSubStack = new CfnInclude(this,`opensearchLambdaStack`, {
      templateFile: cfn_template_file,
      parameters: {
        'AthenaCatalogName': `opensearch-cdk-deployed`,
        'IsVPCAccess': true,
        'SecretNamePrefix': 'asdf',
        'AutoDiscoverEndpoint': false,
        'DomainMapping': `default=${connectionString}`,
        'SecurityGroupIds': [securityGroup.securityGroupId],
        'SubnetIds': [subnet.subnetId],
        'SpillBucket': 'amazon-athena-federation-perf-spill-bucket',
      }
    });
  }
}
