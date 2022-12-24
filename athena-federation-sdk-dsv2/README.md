# Amazon Athena Query Federation SDK DSV2 Adapter Library

This library allows developers to quickly and easily adapt existing Athena Federation connectors into Spark DSV2 connectors.

# How to use

1. Create a pom.xml that depends on: athena-federation-sdk-dsv2, the existing federated connector you want to adapt, and Spark 3.2.1 (or later).

- Be sure to shade the jar and relocate "com.google.common.base" to something like "athena.com.google.common.base" because Spark uses a very old version of it as part of their runtime and it will conflict with the one in the athena-federation-sdk.

2. Provide a AthenaFederationAdapterDefinition for your connector by implementing the interfaces to return what MetadataHandler and RecordHandler your connector uses.

3. Extend the AthenaFederationTableProvider for your connector by defining the getAthenaFederationAdapterDefinition() method to return an instance of the AthenaFederationAdapterDefinition that you defined in (2).

4. Build and test your adapted connector's shaded jar now.

# Examples

For examples on how to use this adapter and/or pom.xml files, see:

- [athena-dynamodb-dsv2](../athena-dynamodb-dsv2)
- [athena-cloudwatch-dsv2](../athena-cloudwatch-dsv2)
- [athena-cloudwatch-metrics-dsv2](../athena-cloudwatch-metrics-dsv2)
- [athena-aws-cmdb-dsv2](../athena-aws-cmdb-dsv2)

# Currently unsupported features

- FederatedIdentity is currently not supported.

