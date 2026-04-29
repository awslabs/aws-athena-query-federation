# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
FROM public.ecr.aws/lambda/java:${JAVA_VERSION} AS builder

ARG JAR_VERSION=2022.47.1
COPY target/athena-sqlserver-${JAR_VERSION}.jar /app/
WORKDIR /app
RUN jar xf athena-sqlserver-${JAR_VERSION}.jar && rm -f athena-sqlserver-${JAR_VERSION}.jar

FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options for ssl
ARG JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/var/lang/lib/security/cacerts"
ENV JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}
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

COPY --from=builder /app/ ${LAMBDA_TASK_ROOT}/

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in .yaml file because legacy and connections use different)
