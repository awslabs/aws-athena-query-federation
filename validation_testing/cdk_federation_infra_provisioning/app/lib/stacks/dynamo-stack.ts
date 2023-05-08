import * as cdk from 'aws-cdk-lib';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import { CfnInclude } from 'aws-cdk-lib/cloudformation-include';
import { Construct } from 'constructs';
import tpcdsJson from '../../resources/tpcds_specs.json'
const path = require('path');
import {FederationStackProps} from './stack-props'

export class DynamoDBStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: FederationStackProps) {
    super(scope, id, props);
    this.init_resources(props!.test_size_gigabytes, props!.s3_path, props!.tpcds_tables, props!.connector_yaml_path);
  }

  init_resources(test_size_gigabytes: number, s3_path: string, tpcds_tables: string[], connector_yaml_path: string) {
    var glue_job_role = new iam.Role(this, 'glue-job-managed-role', {
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("AdministratorAccess")
      ],
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com")
    });
    for (var tableJson of tpcdsJson.tables) {
      var tableName = tableJson.name;
      // skip table if not in the input tpcds_tables
      if (!tpcds_tables.includes(tableName)) {
        continue;
      }
      var primaryKeyArr = tableJson.primary_key;
      if (primaryKeyArr.length == 1) {
        this.initDdbTableWithHashKey(tableName, primaryKeyArr[0]);
      } else {
        // ddb only supports a hash and range key, so if there's > 2 keys, just send the first two
        this.initDdbTableWithHashAndRangeKey(tableName, primaryKeyArr[0], primaryKeyArr[1]);
      }
      // glue job is per table for parallelism
      new glue.Job(this, `dynamodb_glue_job_${tableName}`, {
        executable: glue.JobExecutable.pythonEtl({
          glueVersion: glue.GlueVersion.V4_0,
          pythonVersion: glue.PythonVersion.THREE,
          script: glue.Code.fromAsset(path.join(__dirname, '../../../glue_scripts/dynamodb.py'))
        }),
        role: glue_job_role,
        defaultArguments: {
          '--s3_full_prefix': s3_path,
          '--tpcds_table': tableName 
        }
      });
    }
    const cfn_template_file = connector_yaml_path; 
    const connectorSubStack = new CfnInclude(this, 'DynamoDBLambdaStack', {
      templateFile: cfn_template_file,
      parameters: {
        AthenaCatalogName: 'dynamodb-cdk-deployed',
        SpillBucket: 'amazon-athena-federation-perf-spill-bucket'
      }
    });
  }

  initDdbTableWithHashKey(tableName: string, hashKey: string) {
    new ddb.Table(this, `${tableName}_cdk`, {
      tableName: tableName,
      partitionKey: {
        name: hashKey,
        type: ddb.AttributeType.NUMBER
      },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
  }

  initDdbTableWithHashAndRangeKey(tableName: string, hashKey: string, rangeKey: string) {
    new ddb.Table(this, `${tableName}_cdk`, {
      tableName: tableName,
      partitionKey: {
        name: hashKey,
        type: ddb.AttributeType.NUMBER
      },
      sortKey: {
        name: rangeKey,
        type: ddb.AttributeType.NUMBER
      },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
  }

}
