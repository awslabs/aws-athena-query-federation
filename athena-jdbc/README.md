# Amazon Athena Lambda Jdbc Connector

The JDBC Connector is a compile-only project which is used as a dependency of several other connectors. As a result, this connector can no longer be deployed on its own as of [this PR](https://github.com/awslabs/aws-athena-query-federation/pull/662) from February 2022.

If you used to use this connector to run federated queries agaisnt postgres, mysql, or redshift, you can now directly deploy our connectors for each one: [postgres](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-postgresql), [mysql](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-mysql), and [redshift](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-redshift).