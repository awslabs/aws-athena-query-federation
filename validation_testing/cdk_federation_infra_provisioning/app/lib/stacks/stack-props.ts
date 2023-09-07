import * as cdk from 'aws-cdk-lib';

export interface FederationStackProps extends cdk.StackProps {
  readonly test_size_gigabytes: number;
  readonly s3_path: string; // should be maintained via env var
  readonly tpcds_tables: string[];
  readonly password: string; // should be maintained via env var
  readonly connector_yaml_path: string;
}

