FROM public.ecr.aws/lambda/java:11

# Install necessary tools
RUN yum update -y && yum install -y curl perl openssl

# Copy function code and runtime dependencies from Maven layout
COPY target/athena-docdb-2022.47.1.jar ${LAMBDA_TASK_ROOT}

# Unpack the jar
RUN jar xf athena-docdb-2022.47.1.jar

# Set up environment variables
ENV truststore=${LAMBDA_TASK_ROOT}/rds-truststore.jks
ENV storepassword=federationStorePass

# Download and process the RDS certificate
RUN curl -sS "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem" > ${LAMBDA_TASK_ROOT}/global-bundle.pem && \
    awk 'split_after == 1 {n++;split_after=0} /-----END CERTIFICATE-----/ {split_after=1}{print > "rds-ca-" n ".pem"}' < ${LAMBDA_TASK_ROOT}/global-bundle.pem

# Import certificates into the truststore
RUN for CERT in rds-ca-*; do \
        alias=$(openssl x509 -noout -text -in $CERT | perl -ne 'next unless /Subject:/; s/.*(CN=|CN = )//; print') && \
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

# Set the CMD to your handler
CMD [ "com.amazonaws.athena.connectors.docdb.DocDBCompositeHandler" ]