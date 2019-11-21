# Amazon Athena Query Federation

[![Build Status](https://github.com/awslabs/aws-athena-query-federation/workflows/Java%20CI%20Push/badge.svg)](https://github.com/awslabs/aws-athena-query-federation/actions)

The Amazon Athena Query Federation SDK allows you to customize Amazon Athena with your own code. This enables you to integrate with new data sources, proprietary data formats, or build in new user defined functions. Initially these customizations will be limited to the parts of a query that occur during a TableScan operation but will eventually be expanded to include other parts of the query lifecycle using the same easy to understand interface.

This functionality is currently in **Public Preview** while customers provide us feedback on usability, ease of using the service or building new connectors. We do not recommend that you use these connectors in production or use this preview to make assumptions about the performance of Athena’s Federation features. As we receive more feedback, we will make improvements to the preview and lift raise limits associated with query/connector performance, APIs, SDKs, and user experience. The best way to understand the performance of Athena Data Source Connectors is to run a benchmark when they become generally available (GA) or review our performance guidance.

For more information please consult:

 1. [SDK ReadMe](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/README.md)
 1. [Quick Start Guide](https://github.com/awslabs/aws-athena-query-federation/tree/master/athena-example)
 1. [Available Connectors](https://github.com/awslabs/aws-athena-query-federation/wiki/Available-Connectors)
 1. [Federation Features](Features)
 1. [How To Build A Connector or UDF](How_To_Build_A_Connector_or_UDF)
 1. [Gathering diagnostic info for support](Gather_Diagnostic_Info)
 1. [Frequently Asked Questions](FAQ)
 1. [Common Problems](Common_Problems)
 1. [Installation Pre-requisites](Installation_Prerequisites)
 1. [Known Limitations & Open Issues](Limitations_And_Issues)
 1. [Predicate Pushdown How-To](Predicate-Pushdown-How-To)
 1. [Our Github Wiki](https://github.com/awslabs/aws-athena-query-federation/wiki).

![Architecture Image](https://github.com/awslabs/aws-athena-query-federation/blob/master/docs/img/athena_federation_summary.png?raw=true)

We've written integrations with more than 20 databases, storage formats, and live APIs in order to refine this interface and balance flexibility with ease of use. We hope that making this SDK and initial set of connectors Open Source will allow us to continue to improve the experience and performance of Athena Query Federation.

## Serverless Big Data Using AWS Lambda

![Architecture Image](https://github.com/awslabs/aws-athena-query-federation/blob/master/docs/img/athena_federation_flow.png?raw=true)

### Queries That Span Data Stores

Imagine a hypothetical e-commerce company who's architecture uses:

1. Payment processing in a secure VPC with transaction records stored in HBase on EMR
2. Redis is used to store active orders so that the processing engine can get fast access to them.
3. DocumentDB (e.g. a mongodb compatible store) for Customer account data like email address, shipping addresses, etc..
4. Their e-commerce site using auto-scaling on Fargate with their product catalog in Amazon Aurora.
5. Cloudwatch Logs to house the Order Processor's log events.
6. A write-once-read-many datawarehouse on Redshift.
7. Shipment tracking data in DynamoDB.
8. A fleet of Drivers performing last-mile delivery while utilizing IoT enabled tablets. 
9. Advertising conversion data from a 3rd part cloud provider.

![Architecture Image](https://github.com/awslabs/aws-athena-query-federation/blob/master/docs/img/athena_federation_demo.png?raw=true)

Customer service agents begin receiving calls about orders 'stuck' in a weird state. Some show as pending even though they have delivered, others show as delivered but haven't actually shipped. It would be great if we could quickly run a query across this diverse architecture to understand which orders might be affected and what they have in common.

Using Amazon Athena Query Federation and many of the connectors found in this repository, our hypothetical e-commerce company would be able to run a query that:

1. Grabs all active orders from Redis. (see athena-redis)
2. Joins against any orders with 'WARN' or 'ERROR' events in Cloudwatch logs by using regex matching and extraction. (see athena-cloudwatch)
3. Joins against our EC2 inventory to get the hostname(s) and status of the Order Processor(s) that logged the 'WARN' or 'ERROR'. (see athena-cmdb)
4. Joins against DocumentDB to obtain customer contact details for the affected orders. (see athena-docdb)
5. Joins against DynamoDB to get shipping status and tracking details. (see athena-dynamodb)
6. Joins against HBase to get payment status for the affected orders. (see athena-hbase)


```sql
WITH logs 
     AS (SELECT log_stream, 
                message                                          AS 
                order_processor_log, 
                Regexp_extract(message, '.*orderId=(\d+) .*', 1) AS orderId, 
                Regexp_extract(message, '(.*):.*', 1)            AS log_level 
         FROM 
     "lambda:cloudwatch"."/var/ecommerce-engine/order-processor".all_log_streams 
         WHERE  Regexp_extract(message, '(.*):.*', 1) != 'WARN'), 
     active_orders 
     AS (SELECT * 
         FROM   redis.redis_db.redis_customer_orders), 
     order_processors 
     AS (SELECT instanceid, 
                publicipaddress, 
                state.NAME 
         FROM   awscmdb.ec2.ec2_instances), 
     customer 
     AS (SELECT id, 
                email 
         FROM   docdb.customers.customer_info), 
     addresses 
     AS (SELECT id, 
                is_residential, 
                address.street AS street 
         FROM   docdb.customers.customer_addresses),
     shipments 
     AS ( SELECT order_id, 
                 shipment_id, 
                 from_unixtime(cast(shipped_date as double)) as shipment_time,
                 carrier
        FROM lambda_ddb.default.order_shipments),
     payments
     AS ( SELECT "summary:order_id", 
                 "summary:status", 
                 "summary:cc_id", 
                 "details:network" 
        FROM "hbase".hbase_payments.transactions)
         
SELECT _key_            AS redis_order_id, 
       customer_id, 
       customer.email   AS cust_email, 
       "summary:cc_id"  AS credit_card,
       "details:network" AS CC_type,
       "summary:status" AS payment_status,
       status           AS redis_status, 
       addresses.street AS street_address, 
       shipments.shipment_time as shipment_time,
       shipments.carrier as shipment_carrier,
       publicipaddress  AS ec2_order_processor, 
       NAME             AS ec2_state, 
       log_level, 
       order_processor_log 
FROM   active_orders 
       LEFT JOIN logs 
              ON logs.orderid = active_orders._key_ 
       LEFT JOIN order_processors 
              ON logs.log_stream = order_processors.instanceid 
       LEFT JOIN customer 
              ON customer.id = customer_id 
       LEFT JOIN addresses 
              ON addresses.id = address_id 
       LEFT JOIN shipments
              ON shipments.order_id = active_orders._key_
       LEFT JOIN payments
              ON payments."summary:order_id" = active_orders._key_
```

## License

This project is licensed under the Apache-2.0 License.
