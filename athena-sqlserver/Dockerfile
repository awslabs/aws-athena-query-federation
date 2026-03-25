# Argument for Java version, defaulting to 11
ARG JAVA_VERSION=11
# Use the specified version of Java
FROM public.ecr.aws/lambda/java:${JAVA_VERSION}

# Argument for Java tool options for ssl
ARG JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/var/lang/lib/security/cacerts"
# Set the JAVA_TOOL_OPTIONS environment variable for Java 17
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


# Copy function code and runtime dependencies from Maven layout
COPY target/athena-sqlserver-2022.47.1.jar ${LAMBDA_TASK_ROOT}
# Unpack the jar
RUN jar xf athena-sqlserver-2022.47.1.jar

# Command can be overwritten by providing a different command in the template directly.
# No need to specify here (already defined in .yaml file because legacy and connections use different)
