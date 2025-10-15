# Amazon Athena Lambda Lark Base Connector

This connector enables Amazon Athena to communicate with [Lark Base](https://www.larksuite.com/en_us/product/base) (Feishu Bitable), allowing you to query your Lark Base tables using SQL.

## Features

- **SQL Queries on Lark Base**: Use standard SQL to query Lark Base tables
- **Filter Pushdown**: WHERE clauses are translated to Lark API filters for optimal performance
- **TOP-N Optimization**: ORDER BY + LIMIT queries pushed down to Lark API
- **Parallel Execution**: Large tables automatically split for concurrent processing
- **Flexible Metadata Discovery**: Choose from multiple discovery methods (Glue Catalog, direct Lark Base mapping, Lark Drive)
- **Comprehensive Type Support**: All Lark Base field types supported

## Documentation

For detailed documentation, deployment options, and configuration, please visit the [full project documentation](https://github.com/aganisatria/aws-athena-query-federation-lark).

Key documents:
- [Metadata Discovery Flows](https://github.com/aganisatria/aws-athena-query-federation-lark/blob/main/METADATA_DISCOVERY_FLOWS.md) - Choose your deployment strategy
- [Architecture Guide](https://github.com/aganisatria/aws-athena-query-federation-lark/blob/main/ARCHITECTURE.md) - System design and configuration
- [Visual Diagrams](https://github.com/aganisatria/aws-athena-query-federation-lark/blob/main/DIAGRAMS.md) - Interactive system diagrams

## Glue Lark Base Crawler

This connector supports metadata discovery via AWS Glue. For production deployments, you can use the **Glue Lark Base Crawler** to automatically discover and register Lark Base tables in your AWS Glue Data Catalog.

The crawler code is available in the [aws-athena-query-federation-lark](https://github.com/aganisatria/aws-athena-query-federation-lark) repository under the `glue-lark-base-crawler` module.

**Note**: The glue-lark-base-crawler module is not included in this repository as it is specific to Lark Base metadata discovery and is maintained separately.

## Quick Start

### Prerequisites

- AWS Account with Athena, Lambda, and Glue permissions
- Lark Application with Bitable API access ([Get credentials](https://open.larksuite.com/))
- Java 17 and Maven (for building)
- S3 bucket for Lambda deployment

### Build

```bash
export JAVA_HOME="/path/to/jdk-17"
mvn clean package -pl athena-lark-base -am -Dcheckstyle.skip=true
```

### Deploy

You can deploy using AWS SAM or CloudFormation:

```bash
# Using SAM template
sam deploy --template-file athena-larkbase.yaml \
  --parameter-overrides \
    AthenaCatalogName=lark_base \
    SpillBucket=my-spill-bucket \
    LarkAppSecretManager=my-lark-secret
```

### Query

```sql
-- List databases
SHOW DATABASES IN lark_base;

-- Query data
SELECT * FROM lark_base.my_database.my_table
WHERE status = 'active'
ORDER BY created_date DESC
LIMIT 100;
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `default_secret_manager_lark_app_key` | Yes | AWS Secrets Manager secret containing Lark App ID and Secret |
| `default_page_size` | No | Page size for Lark API calls (default: 500) |
| `default_does_activate_lark_base_source` | No | Enable direct Lark Base table discovery (default: true) |
| `default_does_activate_parallel_split` | No | Enable parallel split execution (default: false) |
| `default_lark_base_sources` | No | Lark Base sources for direct mapping (format: `baseId:tableId,baseId2:tableId2`) |

### Secrets Manager Format

Your Lark credentials should be stored in AWS Secrets Manager in the following format:

```json
{
  "app_id": "cli_xxx",
  "app_secret": "xxx"
}
```

## Metadata Discovery Options

The connector supports multiple metadata discovery methods:

1. **AWS Glue Catalog** (Recommended for production)
    - Use the [Glue Lark Base Crawler](https://github.com/aganisatria/aws-athena-query-federation-lark) to discover and register tables
    - Provides schema caching and versioning

2. **Direct Lark Base Source**
    - Configure `default_lark_base_sources` with your base and table IDs
    - Good for development and testing

3. **Lark Drive Source**
    - Discover tables organized in Lark Drive folders
    - Enable with `default_does_activate_lark_drive_source=true`

See the [Metadata Discovery Flows](https://github.com/aganisatria/aws-athena-query-federation-lark/blob/main/METADATA_DISCOVERY_FLOWS.md) documentation for detailed comparison.

## Performance Optimizations

- **Filter Pushdown**: WHERE clauses are translated to Lark API filters
- **LIMIT Pushdown**: Fetches only required rows
- **TOP-N Optimization**: ORDER BY + LIMIT combined for immediate results
- **Parallel Splits**: Large tables automatically divided for concurrent processing (5-10x faster)

## Supported Types

All Lark Base field types are supported:
- **Basic**: Text, Number, Date, Checkbox, URL, Email, Phone
- **Selection**: Single Select, Multi Select
- **Advanced**: Attachment, Person, Location, Formula
- **Special**: Lookup (with recursive resolution), Duplex Link, Auto Number

## License

This project is licensed under the Apache-2.0 License.

## Links

- [Full Project Repository](https://github.com/aganisatria/aws-athena-query-federation-lark)
- [Glue Lark Base Crawler](https://github.com/aganisatria/aws-athena-query-federation-lark/tree/main/glue-lark-base-crawler)
- [AWS Athena Federation SDK](https://github.com/awslabs/aws-athena-query-federation)
- [Lark Open Platform](https://open.larksuite.com/)
- [Lark Bitable API Documentation](https://open.larksuite.com/document/server-docs/docs/bitable-v1)
