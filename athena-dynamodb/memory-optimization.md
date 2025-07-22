# DynamoDB Connector Memory Optimization Guide

## Environment Variables for Memory Optimization

### 1. Projection Control
```bash
# Disable projection only when absolutely necessary
disable_projection_and_casing=never  # Recommended for memory optimization
disable_projection_and_casing=auto   # Default behavior
disable_projection_and_casing=always # Use only if you have casing issues
```

### 2. Batch Processing
```bash
# Control batch size for processing items (default: 1000)
batch_size=500  # Reduce for lower memory usage
batch_size=2000 # Increase for better performance (higher memory usage)
```

### 3. Page Size Control
```bash
# Control DynamoDB page size (default: 100)
page_size=50   # Reduce for lower memory usage
page_size=200  # Increase for better performance (higher memory usage)
```

### 4. Lambda Memory Configuration
```bash
# Increase Lambda memory allocation
AWS_LAMBDA_FUNCTION_MEMORY_SIZE=1024  # Minimum recommended
AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008  # For large datasets
```

## Memory Optimization Strategies

### 1. **Use Projection (Most Important)**
- Always set `disable_projection_and_casing=never` unless you have specific casing issues
- This reduces data transfer and memory usage by 50-80%

### 2. **Optimize Query Patterns**
- Use specific hash key values instead of table scans when possible
- Apply filters at the DynamoDB level using range key filters
- Limit result sets using LIMIT clauses

### 3. **Tune Batch and Page Sizes**
- Reduce `batch_size` and `page_size` for memory-constrained environments
- Monitor CloudWatch metrics to find optimal values

### 4. **Lambda Configuration**
- Increase Lambda memory allocation (more memory = faster processing)
- Consider using Lambda with larger memory configurations (1024MB+)

### 5. **Schema Optimization**
- Use column name mapping to avoid fetching unnecessary columns
- Avoid complex nested structures when possible

## Monitoring Memory Usage

### CloudWatch Metrics to Monitor:
- `Duration` - Processing time
- `MemoryUtilization` - Memory usage percentage
- `MaxMemoryUsed` - Peak memory consumption
- `Errors` - Out of memory errors

### Troubleshooting High Memory Usage:
1. Check if projection is enabled
2. Reduce batch and page sizes
3. Increase Lambda memory allocation
4. Optimize query patterns to reduce result set size
5. Consider splitting large queries into smaller chunks

## Example Configuration for Memory-Constrained Environment:
```bash
disable_projection_and_casing=never
batch_size=250
page_size=25
AWS_LAMBDA_FUNCTION_MEMORY_SIZE=1024
```

## Example Configuration for Performance-Optimized Environment:
```bash
disable_projection_and_casing=never
batch_size=2000
page_size=500
AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008
```