# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options
ARG JAVA_TOOL_OPTIONS=""
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17 and always include default truststore to avoid SSL handshake issues
ENV JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/var/lang/lib/security/cacerts ${JAVA_TOOL_OPTIONS}"

# Install necessary tools
RUN yum update -y
RUN yum install -y curl perl openssl11

ENV truststore=/var/lang/lib/security/cacerts
ENV storepassword=changeit

# Download and process the RDS certificate
RUN curl -sS "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem" > ${LAMBDA_TASK_ROOT}/global-bundle.pem && \
    awk 'split_after == 1 {n++;split_after=0} /-----END CERTIFICATE-----/ {split_after=1}{print > "rds-ca-" n ".pem"}' < ${LAMBDA_TASK_ROOT}/global-bundle.pem

# Import certificates into the truststore
RUN for CERT in rds-ca-*; do \
        alias=$(openssl11 x509 -noout -text -in $CERT | perl -ne 'next unless /Subject:/; s/.*(CN=|CN = )//; print') && \
        echo "Importing $alias" && \
        keytool -import -file ${CERT} -alias "${alias}" -storepass ${storepassword} -keystore ${truststore} -noprompt && \
        rm $CERT; \
    done

# Clean up
RUN rm ${LAMBDA_TASK_ROOT}/global-bundle.pem

# Optional: List the content of the trust store (for verification)
RUN echo "Trust store content is: " && \
    keytool -list -v -keystore "$truststore" -storepass ${storepassword} | grep Alias | cut -d " " -f3- | while read alias; do \
        expiry=$(keytool -list -v -keystore "$truststore" -storepass ${storepassword} -alias "${alias}" | grep Valid | perl -ne 'if(/until: (.*?)\n/) { print "$1\n"; }'); \
        echo " Certificate ${alias} expires in '$expiry'"; \
    done

# Set the connector version to use
ARG VERSION=2022.47.1

# Copy all jar files from their respective target directories into the Lambda task root.
# Notice how we substitute the version using ${VERSION}.
COPY \
  athena-aws-cmdb/target/athena-aws-cmdb-${VERSION}.jar \
  athena-clickhouse/target/athena-clickhouse-${VERSION}.jar \
  athena-cloudera-hive/target/athena-cloudera-hive-${VERSION}.jar \
  athena-cloudera-impala/target/athena-cloudera-impala-${VERSION}.jar \
  athena-cloudwatch/target/athena-cloudwatch-${VERSION}.jar \
  athena-cloudwatch-metrics/target/athena-cloudwatch-metrics-${VERSION}.jar \
  athena-datalakegen2/target/athena-datalakegen2-${VERSION}.jar \
  athena-db2/target/athena-db2-${VERSION}.jar \
  athena-db2-as400/target/athena-db2-as400-${VERSION}.jar \
  athena-docdb/target/athena-docdb-${VERSION}.jar \
  athena-dynamodb/target/athena-dynamodb-${VERSION}.jar \
  athena-elasticsearch/target/athena-elasticsearch-${VERSION}.jar \
  athena-gcs/target/athena-gcs.zip \
  athena-google-bigquery/target/athena-google-bigquery-${VERSION}.jar \
  athena-hbase/target/athena-hbase-${VERSION}.jar \
  athena-hortonworks-hive/target/athena-hortonworks-hive-${VERSION}.jar \
  athena-kafka/target/athena-kafka-${VERSION}.jar \
  athena-msk/target/athena-msk-${VERSION}.jar \
  athena-mysql/target/athena-mysql-${VERSION}.jar \
  athena-neptune/target/athena-neptune-${VERSION}.jar \
  athena-oracle/target/athena-oracle-${VERSION}.jar \
  athena-postgresql/target/athena-postgresql-${VERSION}.jar \
  athena-redis/target/athena-redis-${VERSION}.jar \
  athena-redshift/target/athena-redshift-${VERSION}.jar \
  athena-saphana/target/athena-saphana.zip \
  athena-snowflake/target/athena-snowflake.zip \
  athena-sqlserver/target/athena-sqlserver-${VERSION}.jar \
  athena-synapse/target/athena-synapse-${VERSION}.jar \
  athena-teradata/target/athena-teradata-${VERSION}.jar \
  athena-timestream/target/athena-timestream-${VERSION}.jar \
  athena-tpcds/target/athena-tpcds-${VERSION}.jar \
  athena-udfs/target/athena-udfs-${VERSION}.jar \
  athena-vertica/target/athena-vertica-${VERSION}.jar \
  ${LAMBDA_TASK_ROOT}/

# Run a shell loop to iterate over all jar/zip files and extract each one.
RUN for file in ${LAMBDA_TASK_ROOT}/*.jar ${LAMBDA_TASK_ROOT}/*.zip; do \
      jar xf "$file" && rm -f "$file"; \
done